from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass, field
from sortedcontainers import SortedDict

@dataclass
class OrderBookState:
    """Complete order book state at a point in time"""
    timestamp: str
    product_id: str
    sequence_num: Optional[int] = None
    bids: SortedDict = field(default_factory=lambda: SortedDict(lambda x: -float(x)))  # Highest price first
    asks: SortedDict = field(default_factory=lambda: SortedDict(lambda x: float(x)))  # Lowest price first

    def _process_messages(self) -> None:
        """Process raw messages into order book states"""
        for msg in self.raw_data:
            events = msg.get('events', [])[0]
            msg_type = events.get('type')
            product_id = events.get('product_id', 'UNKNOWN')
    def process_snapshot(self, msg: Dict, product_id: str) -> None:
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
    def process_update(self, msg: Dict, product_id: str) -> None:
        # Apply updates
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
        """Get depth data for visualization"""
        # Sort bids (highest to lowest) and asks (lowest to highest)
        sorted_bids = sorted(self.bids.items(), reverse=True)[:levels]
        sorted_asks = sorted(self.asks.items())[:levels]

        return sorted_bids, sorted_asks
