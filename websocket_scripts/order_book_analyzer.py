import json
import gzip
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, Callable, List
from datetime import datetime, timezone
import util
from websocket_scripts.order_book_state import OrderBookState

class OrderBookAlertSystem:
    def __init__(self, alert_callback: Optional[Callable[[str, Dict[str, Any]], None]] = None):
        self.previous_stats: Optional[Dict[str, Any]] = None
        self.previous_bids: Optional[Dict[float, float]] = None
        self.previous_asks: Optional[Dict[float, float]] = None
        self.mid_price_history: List[float] = []

        # Callback function to handle alerts (print by default)
        self.alert_callback = alert_callback or self.default_alert_callback

    def default_alert_callback(self, message: str, context: Dict[str, Any]):
        print(f"[ALERT] {message} | {context.get('timestamp', '')}")

    def update(self, order_book: 'OrderBookState', depth_levels: int = 10000):
        stats = order_book.compute_statistics(depth_levels=depth_levels)

        # Track volatility
        mid_price = stats.get("mid_price")
        if mid_price is not None:
            self.mid_price_history.append(mid_price)
            if len(self.mid_price_history) > 20:
                self.mid_price_history.pop(0)

        # Check for all alerts
        self._check_wall_evaporation(order_book, stats, depth_levels)
        self._check_imbalance(stats, depth_levels)
        self._check_spread(stats)
        self._check_mid_price_volatility(stats)

        # Update internal state
        self.previous_stats = stats
        self.previous_bids = dict(order_book.bids)
        self.previous_asks = dict(order_book.asks)

    def _check_wall_evaporation(self, order_book: 'OrderBookState', stats: Dict[str, Any], depth_levels: int):
        if not self.previous_bids or not self.previous_asks:
            return

        current_bids = dict(order_book.bids)
        current_asks = dict(order_book.asks)

        # Check for bid wall evaporation
        for price, prev_size in list(self.previous_bids.items())[:depth_levels]:
            curr_size = current_bids.get(price, 0)
            if prev_size > 0 and curr_size < prev_size * 0.5:
                self.alert_callback("Bid wall evaporated", {
                    "price": price,
                    "previous_size": prev_size,
                    "current_size": curr_size,
                    "side": "bid",
                    "timestamp": stats.get("timestamp")
                })

        # Check for ask wall evaporation
        for price, prev_size in list(self.previous_asks.items())[:depth_levels]:
            curr_size = current_asks.get(price, 0)
            if prev_size > 0 and curr_size < prev_size * 0.5:
                self.alert_callback("Ask wall evaporated", {
                    "price": price,
                    "previous_size": prev_size,
                    "current_size": curr_size,
                    "side": "ask",
                    "timestamp": stats.get("timestamp")
                })

    def _check_imbalance(self, stats: Dict[str, Any], depth_levels: int):
        key = f"imbalance_top_{depth_levels}"
        imbalance = stats.get(key)
        if imbalance is None:
            return
        print(f"Imbalance: {imbalance}")
        if imbalance > 0.9:
            self.alert_callback("Strong BUY imbalance", {
                "imbalance": imbalance,
                "timestamp": stats.get("timestamp")
            })
        elif imbalance < 0.1:
            self.alert_callback("Strong SELL imbalance", {
                "imbalance": imbalance,
                "timestamp": stats.get("timestamp")
            })

    def _check_spread(self, stats: Dict[str, Any]):
        spread = stats.get("spread")
        if not spread:
            return

        # Alert if spread is unusually wide
        if spread > 5:  # can be tuned per market
            self.alert_callback("Spread widened significantly", {
                "spread": spread,
                "timestamp": stats.get("timestamp")
            })

    def _check_mid_price_volatility(self, stats: Dict[str, Any]):
        if len(self.mid_price_history) < 10:
            return

        avg = sum(self.mid_price_history) / len(self.mid_price_history)
        squared_diffs = [(x - avg) ** 2 for x in self.mid_price_history]
        std_dev = (sum(squared_diffs) / len(squared_diffs)) ** 0.5

        # Alert on high mid-price volatility
        if std_dev > 10:  # adjust per asset
            self.alert_callback("Mid-price volatility spike", {
                "std_dev": std_dev,
                "timestamp": stats.get("timestamp")
            })

if __name__ == "__main__":
    snapshots = util.readZIP("../websocket_scripts/data/"
                             "order_book_BTC-USD_2025-08-07T16-35-06.jsonl.gz")

    if not snapshots:
        print("No snapshots found.")
    for i in range(len(snapshots)):
        # Use the latest snapshot (or choose based on index)
        latest_snapshot = snapshots[i]

        # Reconstruct OrderBookState from snapshot
        order_book = OrderBookState(
            timestamp=latest_snapshot["timestamp"],
            product_id=latest_snapshot["product_id"],
            sequence_num=latest_snapshot.get("sequence_num")
        )

        # Restore bids and asks
        for price, size in latest_snapshot["bids"]:
            order_book.bids[float(price)] = float(size)

        for price, size in latest_snapshot["asks"]:
            order_book.asks[float(price)] = float(size)

        alert_system = OrderBookAlertSystem()
        # After each update:
        print(order_book.mid_price)
        alert_system.update(order_book)