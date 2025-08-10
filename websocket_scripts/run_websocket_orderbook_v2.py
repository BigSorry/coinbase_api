import json
import signal
import websocket
import threading
import time
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from pathlib import Path
import queue

# Imports for your order book logic
from order_book_state import OrderBookState
from order_book_classes import LightOrderBookState, FullOrderBookState, BaseOrderBook
from price_history import PriceHistoryTracker
import api_scripts.get_request as api_get

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("../data/websocket/coinbase_ws.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class OrderBookConfig:
    ws_url: str = "wss://advanced-trade-ws.coinbase.com"
    product_ids: List[str] = None
    special_pairs: List[str] = None
    channel_name: str = "level2"
    auto_unsubscribe_after: Optional[int] = None
    reconnect_attempts: int = 5
    reconnect_delay: int = 5
    ping_interval: int = 30
    ping_timeout: int = 10

    def __post_init__(self):
        if self.product_ids is None:
            self.product_ids = ["BTC-USD"]
        if self.special_pairs is None:
            self.special_pairs = []


class OrderBookTracker:
    def __init__(self, config: OrderBookConfig):
        self.config = config
        self.ws_app: Optional[websocket.WebSocketApp] = None
        self.running = threading.Event()
        self.connected = threading.Event()
        self.shutdown_requested = threading.Event()
        self.reconnect_count = 0
        self.order_books: Dict[str, BaseOrderBook] = {}
        self.price_histories: Dict[str, PriceHistoryTracker] = {}
        self.special_pairs = set(config.special_pairs)

    def _create_subscription_message(self) -> str:
        return json.dumps({
            "type": "subscribe",
            "channel": self.config.channel_name,
            "product_ids": self.config.product_ids
        })

    def _create_unsubscription_message(self) -> str:
        return json.dumps({
            "type": "unsubscribe",
            "channel": self.config.channel_name,
            "product_ids": self.config.product_ids
        })

    def _on_open(self, ws):
        logger.info("WebSocket connection established")
        self.connected.set()
        self.reconnect_count = 0
        try:
            ws.send(self._create_subscription_message())
            logger.info(f"Subscribed to {self.config.channel_name} for {self.config.product_ids}")
        except Exception as e:
            logger.error(f"Failed to send subscription message: {e}")

    def _on_message(self, ws, message):
        try:
            data = json.loads(message)
            data["received_at"] = datetime.now(timezone.utc).isoformat()
            self._process_message(data)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode JSON message: {e}")
        except Exception as e:
            logger.error(f"Error processing message: {e}")

    def createPriceHistoryTracker(self, product_id: str, timestamp_str : str) -> None:
        if product_id in self.special_pairs:
            self.price_histories[product_id] = PriceHistoryTracker(
                product_id=product_id,
                min_change_pct=0.005,
                min_change_abs=0.01,
                min_time_interval=2,
                max_size=15000,
                output_file=Path(f"./data/special_track_prices/prices_{product_id}_{timestamp_str}.jsonl")
            )
        else:
            self.price_histories[product_id] = PriceHistoryTracker(
                product_id=product_id,
                output_file=Path(f"./data/track_prices/prices_{product_id}_{timestamp_str}.jsonl")
            )   # default settings
        logger.info(f"Price history tracker initialized for {product_id}")

    def createOrderBookState(self, data: Dict[str, Any], product_id: str, timestamp_str : str) -> None:
        if product_id in self.special_pairs:
            self.order_books[product_id] = FullOrderBookState(
                timestamp=data["received_at"],
                product_id=product_id,
                sequence_num=data.get("sequence"),
                output_file=Path(f"./data/order_book_{product_id}_{timestamp_str}.jsonl")
            )
        else:
            self.order_books[product_id] = LightOrderBookState(
                timestamp=data["received_at"],
                product_id=product_id,
                sequence_num=data.get("sequence"),
                output_file=Path(f"./data/order_book_{product_id}_{timestamp_str}.jsonl")
            )
        self.order_books[product_id].process_snapshot(data)
        logger.info(f"Order book initialized for {product_id}")

    def updateOrderBookState(self, product_id: str, data: Dict[str, Any]) -> None:
        current_book = self.order_books.get(product_id)
        if current_book:
            current_book.process_update(data)
            # Save only if it's a special pair and write interval is due
            if product_id in self.special_pairs:
                prev_time = current_book.last_write_time
                current_book.write_if_due()
                if prev_time != current_book.last_write_time:
                    logger.info(f"Order book saved for {product_id}")

    def _process_message(self, data: Dict[str, Any]):
        msg_type = data["events"][0].get("type")
        product_id = data["events"][0].get("product_id", "")

        if msg_type == "subscriptions":
            logger.info(f"Subscription confirmed: {data}")

        elif msg_type == "snapshot":
            timestamp_str = data["received_at"].replace(":", "-").replace("+", "_").split(".")[0]
            self.createOrderBookState(data, product_id, timestamp_str)
            self.createPriceHistoryTracker(product_id, timestamp_str)

        elif msg_type == "update":
            # Feed mid price into price history
            if product_id in self.price_histories:
                self.updateOrderBookState(product_id, data)
                # TODO Uncouple price history from order book state
                self.price_histories[product_id].record(self.order_books[product_id].mid_price)
                self.price_histories[product_id].write_prices()

        elif msg_type == "error":
            logger.error(f"WebSocket error message: {data}")
        else:
            logger.debug(f"Unknown message type: {msg_type}")

    def _on_error(self, ws, error):
        logger.error(f"WebSocket error: {error}")
        self.connected.clear()

    def _on_close(self, ws, code, msg):
        logger.info(f"WebSocket closed (code: {code}, msg: {msg})")
        self.connected.clear()
        if not self.shutdown_requested.is_set() and self.running.is_set():
            self._attempt_reconnection()

    def _attempt_reconnection(self):
        if self.reconnect_count >= self.config.reconnect_attempts:
            logger.error("Max reconnection attempts reached. Shutting down.")
            self.shutdown()
            return
        self.reconnect_count += 1
        delay = min(self.config.reconnect_delay * (2 ** (self.reconnect_count - 1)), 60)
        logger.info(f"Attempting reconnection in {delay}s")
        time.sleep(delay)
        if not self.shutdown_requested.is_set():
            self._start_websocket()

    def _start_websocket(self):
        try:
            self.ws_app = websocket.WebSocketApp(
                self.config.ws_url,
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close
            )
            self.ws_app.run_forever(
                ping_interval=self.config.ping_interval,
                ping_timeout=self.config.ping_timeout
            )
        except Exception as e:
            logger.error(f"Failed to start WebSocket: {e}")
            if not self.shutdown_requested.is_set():
                self._attempt_reconnection()

    def start_blocking(self):
        if self.running.is_set():
            logger.warning("Tracker already running")
            return
        self.running.set()
        self._start_websocket()

    def unsubscribe(self):
        if self.ws_app and self.connected.is_set():
            try:
                self.ws_app.send(self._create_unsubscription_message())
                logger.info("Unsubscription message sent")
            except Exception as e:
                logger.error(f"Failed to send unsubscribe message: {e}")

    def shutdown(self):
        if self.shutdown_requested.is_set():
            return
        self.shutdown_requested.set()
        self.running.clear()
        self.unsubscribe()
        if self.ws_app:
            self.ws_app.close()


# --- Helpers ---
def chunk_list(lst, chunk_size):
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]


# --- Signal handling ---
shutdown_event = threading.Event()
def signal_handler(signum, frame):
    logger.info(f"Signal {signum} received, shutting down...")
    shutdown_event.set()


# --- Per-batch runner ---
def run_tracker_for_batch(product_batch, special_pairs):
    orderbook_config = OrderBookConfig(
        product_ids=product_batch,
        special_pairs=special_pairs,
        channel_name="level2"
    )
    tracker = OrderBookTracker(orderbook_config)
    try:
        tracker.start_blocking()
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        tracker.shutdown()


# --- Main ---
def main():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    special_pairs = ["BTC-USD", "ETH-USD", "SOL-USD", "ADA-USD", "XRP-USD"]
    usdc_pairs = api_get.getTradePairs(fiat_currency="USD")


    max_per_ws = 20
    threads = []

    for batch in chunk_list(usdc_pairs, max_per_ws):
        t = threading.Thread(target=run_tracker_for_batch, args=(batch, special_pairs))
        t.start()
        threads.append(t)
        time.sleep(1)  # stagger connections to avoid rate limits

    # Wait for shutdown signal
    shutdown_event.wait()
    logger.info("Shutdown signal received, stopping trackers...")

    for t in threads:
        t.join()


if __name__ == "__main__":
    main()
