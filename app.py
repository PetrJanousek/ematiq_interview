import asyncio
from contextlib import suppress
import datetime
import json
from typing import Optional, AsyncGenerator, TypedDict
import aiohttp
import time
import websockets
import logging as log
import os
from dotenv import load_dotenv
from websockets import WebSocketClientProtocol
from websockets.typing import Data
import websockets.typing

load_dotenv()

log.basicConfig(
    level=log.DEBUG,
    format="%(asctime)s,%(msecs)d %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)

log.getLogger("websockets").setLevel(log.ERROR)

HEARTBEAT_LIMIT: int = 2
HEARTBEAT_INTERVAL: int = 1
EXCHANGE_RATE_UPDATE_INTERVAL: int = 7200

Rates = dict[str, dict[str, float]]


class Payload(TypedDict):
    marketId: int
    selectionId: int
    odds: float
    stake: float
    currency: str
    date: str


class Message(TypedDict):
    type: str
    id: int
    payload: Payload


class HeartbeatException(Exception):
    """Custom exception for heartbeat errors."""

    pass


def truncate_float(number: float, length: int):
    """Truncate float numbers, up to the number specified
    in length that must be an integer.
    - Copied from SO
    """

    res: float = number * pow(10, length)
    res = int(res)
    res = float(res)
    res /= pow(10, length)
    return number


class Timer:
    """Simple timer that times the exact sleep duration so time drifting doesn't occur."""

    def __init__(self, interval: int):
        self.interval: int = interval
        self.start_time: float = time.monotonic()

    def duration(self) -> float:
        return self.interval - ((time.monotonic() - self.start_time) % self.interval)


class Connection:
    """Handles connection to specified websocket and it's handling."""

    def __init__(self, uri: str):
        self.uri: str = uri
        self.ws: Optional[WebSocketClientProtocol] = None

    async def establish_connection(self):
        """Establishes connection to websocket"""
        log.debug(f"Trying to connect to {self.uri}")
        self.ws = await websockets.connect(self.uri)
        log.info(f"New connection established")

    async def send_message(self, message: dict | Message) -> None:
        """Sends message through websocket it's connected to"""
        if self.ws:
            log.debug(f"Sending message: {message}")
            await self.ws.send(json.dumps(message))

    async def receive_messages(self) -> AsyncGenerator[Data, None]:
        """Starts receiving messages and yields them out"""
        if self.ws:
            async for message in self.ws:
                log.debug(f"Received message: {message!r}")
                yield message

    async def close(self) -> None:
        """Closes the websocket if any connection exists"""
        if self.ws:
            await self.ws.close()

    async def is_closed(self) -> bool:
        """Checks if connection exists"""
        if self.ws:
            return self.ws.closed
        return False


class AppContext:
    """Provides context for the running app and handling all related logic."""

    def __init__(
        self, connection: Connection, historical_rates_base: str, live_rates_url: str
    ):
        self.queue: asyncio.Queue[Message] = asyncio.Queue()
        self.last_heartbeat: float = 0.0
        self.exchange_rates: Rates = {}
        self.connection: Connection = connection
        self.historical_rates_base: str = historical_rates_base
        self.live_rates_url: str = live_rates_url

    async def run(self):
        """App's main loop, handling all logic"""
        try:
            asyncio.create_task(self.message_handler())
            asyncio.create_task(self.request_live_exchange_rates_periodically())

            while True:
                try:
                    await self.connection.establish_connection()
                    recv_task: asyncio.Task = asyncio.create_task(self.receiver())
                    heartbeat_task: asyncio.Task = asyncio.create_task(self.heartbeat())

                    await asyncio.gather(recv_task, heartbeat_task)
                except HeartbeatException:
                    log.warning(
                        "Restarting websocket connection on heartbeat limit reached"
                    )
                    with suppress(asyncio.CancelledError):
                        log.debug("cancelling recv_task")
                        recv_task.cancel()
                        if recv_task:
                            # Ensure it is not unbound
                            await recv_task
                    log.info("finished cancelling recv_task")
                except OSError:
                    log.error("Error trying to connect, will retry")
                finally:
                    await self.connection.close()
                    log.warning("Closed the connection")

        except:
            await self.connection.close()
            log.info("Run is shutting down")

    async def heartbeat(self) -> bool:
        """Checks heartbeat limit and sends heartbeat to the other side.
        On limit breach raises Exception"""
        log.info("Starting heartbeat check")
        self.last_heartbeat = time.time()
        timer: Timer = Timer(HEARTBEAT_INTERVAL)
        while True:
            if time.time() - self.last_heartbeat >= HEARTBEAT_LIMIT:
                log.info("Heartbeat missed, closing connection...")
                raise HeartbeatException
            await self.connection.send_message({"type": "heartbeat"})
            await asyncio.sleep(timer.duration())

    async def receiver(self) -> None:
        """Receives messages from connection and puts it into queue"""
        log.info("Starting to listen to new messages")
        if not self.connection:
            return
        async for message in self.connection.receive_messages():
            m: Message = json.loads(message)
            asyncio.create_task(self.queue.put(m))

    async def message_handler(self) -> None:
        """Retrieves message from queue and handles it"""
        while True:
            msg: Message = await self.queue.get()
            log.info(f"Consumed message: {msg}")
            match msg["type"]:
                case "heartbeat":
                    self.last_heartbeat = time.time()
                    log.info("Received heartbeat")
                case "message":
                    asyncio.create_task(self.process_message(msg))
                case _:
                    log.warning(
                        f"Unsupported message type: {msg['type']} received, ignoring"
                    )

    async def process_message(self, message: Message) -> None:
        """Processes incoming message and sends response"""
        log.debug("Processing new message")
        try:
            pld: Payload = message["payload"]
            # trim date into "YYYY-MM-DD"
            date: str = pld["date"].split("T", 1)[0]
            currency: str = pld["currency"]

            if currency == "EUR":
                await self.connection.send_message(message)
                return

            await self.ensure_rates_for_date(date)

            rate_key: str = f"EUR{currency}"

            if rate_key in self.exchange_rates[date]:
                pld["stake"] = truncate_float(
                    pld["stake"] / self.exchange_rates[date][rate_key], 5
                )
                pld["currency"] = "EUR"
                await self.connection.send_message(message)
            else:
                raise ValueError(f"Rate for currency: {currency} doesnt exist")

        except ValueError as e:
            log.warn(f"Processing message failed with: {e}")
            await self.connection.send_message(
                create_error_message(message["id"], str(e))
            )

        except Exception as e:
            log.info(f"Something went wrong with processing message: {e}")

    async def ensure_rates_for_date(self, date: str) -> None:
        """Check if rates for date are cached, if not request it"""

        if not date in self.exchange_rates:
            log.debug(
                f"Historical rates for date: {date} not found, trying to fetch it"
            )
            rates: Optional[Rates] = await self.request_historical_exchange_rates(date)
            if rates:
                self.exchange_rates.update(rates)
                log.info("Exchange rates updated successfuly")
            else:
                log.warn(f"Failed to get historical rates for date: {date}")
                raise ValueError("Historical rates for date: {date} doesnt exist")

    async def request_historical_exchange_rates(self, date: str) -> Optional[Rates]:
        """Request historical exchange rates and update the cached rates"""
        try:
            rates: dict[str, float] = await request_exchange_rates(
                self.historical_rates_base, date
            )
            log.debug(f"Historical rates for date: {date} received")
            return {date: rates}
        except Exception as e:
            log.error(f"Failed to fetch historical exchange rates. Error: {e}")
        return None

    async def request_live_exchange_rates_periodically(self) -> None:
        """Periodically requests exchange rates via api call"""
        timer: Timer = Timer(EXCHANGE_RATE_UPDATE_INTERVAL)
        while True:
            try:
                rates: dict[str, float] = await request_exchange_rates(
                    self.live_rates_url
                )
                current_date: str = datetime.datetime.now().strftime("%Y-%m-%d")
                self.exchange_rates[current_date] = rates
                log.debug("Newest exchange rates updated")
                await asyncio.sleep(timer.duration())
            except Exception as e:
                log.error(f"Failed to update live rates, will retry later. Error: {e}")
                await asyncio.sleep(10)


async def request_exchange_rates(
    url: str, date: Optional[str] = None
) -> dict[str, float]:
    """Requests newest exchange rates from provided api,
    optionally requests historical rates"""
    if date:
        url = f"{url}&date={date}"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return (await response.json())["quotes"]
    raise Exception("Unable to get response")


def create_error_message(message_id: int, error_str: str) -> dict[str, str | int]:
    """Creates dict with error message"""

    return {
        "type": "error",
        "id": message_id,
        "message": f"Unable to convert stake. Error: {error_str}",
    }


async def main():

    historical_rates_api_key = os.getenv("EXCHANGERATE_API_KEY", None)

    if not historical_rates_api_key:
        log.info("Unable to get api key, shutting down application")
        return

    historical_url_base = f"https://api.exchangerate.host/historical?access_key={historical_rates_api_key}&source=EUR"
    live_rates_url = f"https://api.exchangerate.host/live?access_key={historical_rates_api_key}&source=EUR"

    conn = Connection("wss://currency-assignment.ematiq.com")
    ctx = AppContext(conn, historical_url_base, live_rates_url)

    try:
        await ctx.run()
    finally:
        log.info("App shutted down")


if __name__ == "__main__":
    asyncio.run(main())
