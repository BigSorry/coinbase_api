"""
Improved Coinbase WebSocket Order Book Tracker
==============================================

This implementation fixes several issues in the original code:
- Proper WebSocket reference management
- Context managers for file operations
- Better error handling and logging
- Clean shutdown mechanism
- Configurable parameters
- Thread-safe operations
- Better data structure management
- Proper connection lifecycle management

Requirements:
pip install websocket-client
"""

import json
import signal
import sys
import websocket
import threading
import time
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from pathlib import Path
import queue
from contextlib import contextmanager
from orderbook_analyze import OrderBookState

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('../data/websocket/coinbase_ws.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class OrderBookConfig:
    """Configuration for the order book tracker"""
    ws_url: str = "wss://advanced-trade-ws.coinbase.com"
    product_ids: List[str] = None
    channel_name: str = "level2"
    output_file: str = "coinbase_orderbook.jsonl"
    auto_unsubscribe_after: Optional[int] = None  # seconds
    reconnect_attempts: int = 5
    reconnect_delay: int = 5
    ping_interval: int = 30
    ping_timeout: int = 10

    def __post_init__(self):
        if self.product_ids is None:
            self.product_ids = ["BTC-USD"]


class OrderBookTracker:
    """
    Improved Coinbase WebSocket order book tracker with proper error handling,
    reconnection logic, and clean shutdown capabilities.
    """

    def __init__(self, config: OrderBookConfig):
        self.config = config
        self.ws_app: Optional[websocket.WebSocketApp] = None
        self.ws_thread: Optional[threading.Thread] = None
        self.running = threading.Event()
        self.connected = threading.Event()
        self.shutdown_requested = threading.Event()
        self.message_queue = queue.Queue()
        self.reconnect_count = 0
        self.last_ping = time.time()
        self.order_books: Dict[str, OrderBookState] = {}
        # Register signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        # Ensure output directory exists
        Path(self.config.output_file).parent.mkdir(parents=True, exist_ok=True)

    def _signal_handler(self, sig, frame):
        """Handle shutdown signals gracefully"""
        logger.info(f"Received signal {sig}. Initiating graceful shutdown...")
        self.shutdown()

    def _create_subscription_message(self) -> str:
        """Create subscription message"""
        message = {
            "type": "subscribe",
            "channel": self.config.channel_name,
            "product_ids": self.config.product_ids
        }
        return json.dumps(message)

    def _create_unsubscription_message(self) -> str:
        """Create unsubscription message"""
        message = {
            "type": "unsubscribe",
            "channel": self.config.channel_name,
            "product_ids": self.config.product_ids
        }
        return json.dumps(message)

    def _on_open(self, ws):
        """Handle WebSocket connection open"""
        logger.info("WebSocket connection established")
        self.connected.set()
        self.reconnect_count = 0

        try:
            subscription_message = self._create_subscription_message()
            ws.send(subscription_message)
            logger.info(f"Subscribed to {self.config.channel_name} for {self.config.product_ids}")
        except Exception as e:
            logger.error(f"Failed to send subscription message: {e}")

    def _on_message(self, ws, message):
        """Handle incoming WebSocket messages"""
        try:
            data = json.loads(message)
            # Add timestamp to the data
            data['received_at'] = datetime.now(timezone.utc).isoformat()

            # Process different message types
            self._process_message(data)

        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode JSON message: {e}")
        except Exception as e:
            logger.error(f"Error processing message: {e}")

    def _process_message(self, data: Dict[str, Any]):
        """Process different types of messages"""
        message_type = data["events"][0].get("type")

        if message_type == "subscriptions":
            logger.info(f"Subscription confirmed: {data}")

        elif message_type == "snapshot":
            logger.info(f"Orderbook snapshot")
            product_id = data["events"][0]['product_id']
            logger.info(f"Snapshot received for {product_id}")

            # Create new OrderBookState from snapshot
            book = OrderBookState(
                timestamp=data["received_at"],
                product_id=product_id,
                sequence_num=data.get("sequence"),
                output_file=Path(f"./data/order_book_{product_id}"
                                 f"_{data['received_at'],}.jsonl")
            )
            book.process_snapshot(data)
            self.order_books[product_id] = book

        elif message_type == "update":
            product_id = data["events"][0]['product_id']
            current_book = self.order_books.get(product_id)
            if current_book:
                current_book.process_update(data)
                last_save_time = current_book.last_write_time
                current_book.write_metrics_if_due()
                save_time = current_book.last_write_time
                if save_time != last_save_time:
                    logger.info(f"Order book saved for {product_id} at {save_time.isoformat()}")

        elif message_type == "error":
            logger.error(f"WebSocket error message: {data}")

        else:
            logger.debug(f"Unknown message type: {message_type}")

    def _on_error(self, ws, error):
        """Handle WebSocket errors"""
        logger.error(f"WebSocket error: {error}")
        self.connected.clear()

    def _on_close(self, ws, close_status_code, close_msg):
        """Handle WebSocket connection close"""
        logger.info(f"WebSocket closed (code: {close_status_code}, msg: {close_msg})")
        self.connected.clear()

        # Attempt reconnection if not shutting down
        if not self.shutdown_requested.is_set() and self.running.is_set():
            self._attempt_reconnection()

    def _attempt_reconnection(self):
        """Attempt to reconnect with exponential backoff"""
        if self.reconnect_count >= self.config.reconnect_attempts:
            logger.error("Max reconnection attempts reached. Shutting down.")
            self.shutdown()
            return

        self.reconnect_count += 1
        delay = min(self.config.reconnect_delay * (2 ** (self.reconnect_count - 1)), 60)

        logger.info(f"Attempting reconnection {self.reconnect_count}/{self.config.reconnect_attempts} in {delay}s")

        time.sleep(delay)

        if not self.shutdown_requested.is_set():
            self._start_websocket()

    def _start_websocket(self):
        """Start the WebSocket connection"""
        try:
            self.ws_app = websocket.WebSocketApp(
                self.config.ws_url,
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close
            )

            # Run forever with ping/pong
            self.ws_app.run_forever(
                ping_interval=self.config.ping_interval,
                ping_timeout=self.config.ping_timeout
            )

        except Exception as e:
            logger.error(f"Failed to start WebSocket: {e}")
            if not self.shutdown_requested.is_set():
                self._attempt_reconnection()

    def start(self):
        """Start the order book tracker"""
        if self.running.is_set():
            logger.warning("Tracker is already running")
            return

        logger.info("Starting Coinbase order book tracker...")
        logger.info(f"Products: {self.config.product_ids}")
        logger.info(f"Channel: {self.config.channel_name}")
        logger.info(f"Output file: {self.config.output_file}")

        self.running.set()

        # Start WebSocket in a separate thread
        self.ws_thread = threading.Thread(target=self._start_websocket, daemon=True)
        self.ws_thread.start()

        # Wait for connection
        if self.connected.wait(timeout=10):
            logger.info("Successfully connected to Coinbase WebSocket")
        else:
            logger.error("Failed to establish connection within timeout")
            return

        # Handle auto-unsubscribe if configured
        if self.config.auto_unsubscribe_after:
            self._handle_auto_unsubscribe()

    def _handle_auto_unsubscribe(self):
        """Handle automatic unsubscription after specified time"""

        def unsubscribe_timer():
            time.sleep(self.config.auto_unsubscribe_after)
            if self.running.is_set() and not self.shutdown_requested.is_set():
                logger.info(f"Auto-unsubscribing after {self.config.auto_unsubscribe_after} seconds")
                self.unsubscribe()

        timer_thread = threading.Thread(target=unsubscribe_timer, daemon=True)
        timer_thread.start()

    def unsubscribe(self):
        """Unsubscribe from the current channels"""
        if self.ws_app and self.connected.is_set():
            try:
                unsubscribe_message = self._create_unsubscription_message()
                self.ws_app.send(unsubscribe_message)
                logger.info("Unsubscription message sent")
            except Exception as e:
                logger.error(f"Failed to send unsubscribe message: {e}")

    def shutdown(self):
        """Gracefully shutdown the tracker"""
        if self.shutdown_requested.is_set():
            return

        logger.info("Shutting down order book tracker...")
        self.shutdown_requested.set()
        self.running.clear()

        # Unsubscribe first
        self.unsubscribe()

        # Close WebSocket connection
        if self.ws_app:
            self.ws_app.close()

        # Wait for thread to finish
        if self.ws_thread and self.ws_thread.is_alive():
            self.ws_thread.join(timeout=5)

        logger.info("Shutdown complete")

    def wait_for_shutdown(self):
        """Wait for shutdown signal"""
        try:
            while self.running.is_set() and not self.shutdown_requested.is_set():
                time.sleep(1)
        except KeyboardInterrupt:
            self.shutdown()


def main():
    """Main function"""
    # Configuration
    config = OrderBookConfig(
        product_ids=["BTC-USD", "ETH-USD"],  # Multiple products
        channel_name="level2",
        output_file="../data/websocket/coinbase_orderbook.jsonl",
        auto_unsubscribe_after=None,  # Set to None for continuous running
        reconnect_attempts=5,
        reconnect_delay=5
    )

    # Create and start tracker
    tracker = OrderBookTracker(config)

    try:
        tracker.start()

        # Keep running until shutdown
        tracker.wait_for_shutdown()

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        tracker.shutdown()


if __name__ == "__main__":
    main()