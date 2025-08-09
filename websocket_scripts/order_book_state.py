import json
import gzip
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass, field
from sortedcontainers import SortedDict
from pathlib import Path
@dataclass
class OrderBookState:
    """Complete order book state at a point in time"""
    timestamp: str
    product_id: str
    sequence_num: Optional[int] = None
    bids: SortedDict = field(default_factory=lambda: SortedDict(lambda x: -float(x)))  # Highest price first
    asks: SortedDict = field(default_factory=lambda: SortedDict(lambda x: float(x)))  # Lowest price first
    
    # ⏱️ Track last write time
    last_write_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    write_interval: int = 60  # seconds (configurable)

    output_file: Optional[Path] = None  # set by caller

    def process_snapshot(self, msg: Dict) -> None:
        """Process snapshot message"""
        update_list = msg.get('events', [])[0]['updates']
        # Parse bids and asks
        for update_item in update_list:
            side = update_item.get('side')
            price = float(update_item.get('price_level', 0))
            size = float(update_item.get('new_quantity', 0))
            if size > 0:
                if side == 'bid':
                    self.bids[price] = size
                elif side == 'offer':
                    self.asks[price] = size

    def process_meta_data(self, msg: Dict) -> None:
        # Set sequence number and timestamp
        self.sequence_num = msg.get('sequence_num', -1)
        self.timestamp = msg.get('received_at', datetime.now(timezone.utc).isoformat())

    def process_update(self, msg: Dict) -> None:
        # Apply updates
        self.process_meta_data(msg)
        update_list = msg.get('events', [])[0]['updates']
        for update_item in update_list:
            side = update_item.get('side')
            price = float(update_item.get('price_level', 0))
            size = float(update_item.get('new_quantity', 0))

            if side == 'bid':
                if size == 0:
                    self.bids.pop(price, None)
                else:
                    self.bids[price] = size
            elif side == 'offer':
                if size == 0:
                    self.asks.pop(price, None)
                else:
                    self.asks[price] = size

    @property
    def best_bid(self) -> Optional[float]:
        return max(self.bids.keys()) if self.bids else None

    @property
    def best_ask(self) -> Optional[float]:
        return min(self.asks.keys()) if self.asks else None

    @property
    def spread(self) -> Optional[float]:
        if self.best_bid and self.best_ask:
            return self.best_ask - self.best_bid
        return None

    @property
    def mid_price(self) -> Optional[float]:
        if self.best_bid and self.best_ask:
            return (self.best_bid + self.best_ask) / 2
        return None

    def get_depth_data(self, levels: int = 50) -> Tuple[List[Tuple[float, float]], List[Tuple[float, float]]]:
        """Get top N levels of bids and asks for depth visualization"""
        bids = list(self.bids.items())[:levels]
        asks = list(self.asks.items())[:levels]
        return bids, asks

    def write_metrics_if_due(self):
        """Write best bid/ask/spread to file if enough time has passed."""
        now = datetime.now(timezone.utc)

        if (now - self.last_write_time).total_seconds() >= self.write_interval:
            self.last_write_time = now

            if not self.output_file:
                return  # Skip if output path not set

            data_order_book = {
                "timestamp": now.isoformat(),
                "product_id": self.product_id,
                "sequence_num": self.sequence_num,
                "bids": list(self.bids.items()),
                "asks": list(self.asks.items()),
            }

            try:
                self.output_file.parent.mkdir(parents=True, exist_ok=True)
                # with open(self.output_file, 'a', encoding='utf-8') as f:
                #     f.write(json.dumps(data_order_book) + '\n')
                with gzip.open(f"{self.output_file}.gz", "at", encoding='utf-8') as f:
                    f.write(json.dumps(data_order_book) + '\n')
            except Exception as e:
                print(f"[OrderBookState] Failed to write metrics: {e}")

    def compute_statistics(self, depth_levels: int = 500) -> Dict[str, Any]:
        """Compute key order book statistics from current state."""
        bids_list = list(self.bids.items())[:depth_levels]
        asks_list = list(self.asks.items())[:depth_levels]

        bid_volumes = [size for _, size in bids_list]
        ask_volumes = [size for _, size in asks_list]

        bid_volume_sum = sum(bid_volumes)
        ask_volume_sum = sum(ask_volumes)

        imbalance = None
        if (bid_volume_sum + ask_volume_sum) > 0:
            imbalance = bid_volume_sum / (bid_volume_sum + ask_volume_sum)

        stats = {
            "timestamp": self.timestamp,
            "product_id": self.product_id,
            "spread": self.spread,
            "mid_price": self.mid_price,
            "best_bid": self.best_bid,
            "best_bid_size": self.bids[self.best_bid] if self.best_bid in self.bids else None,
            "best_ask": self.best_ask,
            "best_ask_size": self.asks[self.best_ask] if self.best_ask in self.asks else None,
            "imbalance_top_{}".format(depth_levels): imbalance,
            "total_bid_volume_top_{}".format(depth_levels): bid_volume_sum,
            "total_ask_volume_top_{}".format(depth_levels): ask_volume_sum,
        }

        return stats