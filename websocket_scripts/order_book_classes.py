from datetime import datetime, timezone
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Tuple, List, Optional, Any
from sortedcontainers import SortedDict
import gzip
import json
@dataclass
class BaseOrderBook:
    timestamp: str
    product_id: str
    sequence_num: Optional[int] = None
    last_write_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    write_interval: int = 60
    output_file: Optional[Path] = None
    bids: SortedDict = field(default_factory=lambda: SortedDict(lambda x: -float(x)))
    asks: SortedDict = field(default_factory=lambda: SortedDict(lambda x: float(x)))

    def process_meta_data(self, msg: Dict) -> None:
        self.sequence_num = msg.get('sequence_num', -1)
        self.timestamp = msg.get('received_at', datetime.now(timezone.utc).isoformat())

    @property
    def best_bid(self) -> Optional[float]:
        # SortedDict with key function -x keeps highest price at index 0
        return self.bids.peekitem(0)[0] if self.bids else None

    @property
    def best_ask(self) -> Optional[float]:
        # SortedDict with key function x keeps lowest price at index 0
        return self.asks.peekitem(0)[0] if self.asks else None

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

    def imbalance(self):
        bid_vol = sum(self.bids.values())
        ask_vol = sum(self.asks.values())
        total = bid_vol + ask_vol
        return bid_vol / total if total > 0 else None

    def write_if_due(self):
        now = datetime.now(timezone.utc)
        if (now - self.last_write_time).total_seconds() >= self.write_interval:
            self.last_write_time = now
            self._write_snapshot(now)

    def _write_snapshot(self, now: datetime):
        raise NotImplementedError


# -------- FULL MODE --------
@dataclass
class FullOrderBookState(BaseOrderBook):
    def process_snapshot(self, msg: Dict) -> None:
        for u in msg.get('events', [])[0]['updates']:
            side = u.get('side')
            price = float(u.get('price_level', 0))
            size = float(u.get('new_quantity', 0))
            if side == 'bid':
                self.bids[price] = size
            elif side == 'offer':
                self.asks[price] = size

    def process_update(self, msg: Dict) -> None:
        self.process_meta_data(msg)
        for u in msg.get('events', [])[0]['updates']:
            side = u.get('side')
            price = float(u.get('price_level', 0))
            size = float(u.get('new_quantity', 0))
            book = self.bids if side == 'bid' else self.asks
            if size == 0:
                book.pop(price, None)
            else:
                book[price] = size

    def _write_snapshot(self, now: datetime):
        if not self.output_file:
            return
        self.output_file.parent.mkdir(parents=True, exist_ok=True)
        data_order_book = {
            "timestamp": now.isoformat(),
            "product_id": self.product_id,
            "sequence_num": self.sequence_num,
            "bids": list(self.bids.items()),
            "asks": list(self.asks.items()),
        }
        try:
            self.output_file.parent.mkdir(parents=True, exist_ok=True)
            with gzip.open(f"{self.output_file}.gz", "at", encoding='utf-8') as f:
                f.write(json.dumps(data_order_book) + '\n')
        except Exception as e:
            print(f"[OrderBookState] Failed to write metrics: {e}")


# -------- LIGHT MODE --------
@dataclass
class LightOrderBookState(BaseOrderBook):
    top_n: int = 10
    write_interval: int = 60

    def process_snapshot(self, msg: Dict) -> None:
        for u in msg.get('events', [])[0]['updates']:
            side = u.get('side')
            price = float(u.get('price_level', 0))
            size = float(u.get('new_quantity', 0))
            if side == 'bid':
                self.bids[price] = size
            elif side == 'offer':
                self.asks[price] = size

    def process_update(self, msg: Dict) -> None:
        self.process_meta_data(msg)
        for u in msg.get('events', [])[0]['updates']:
            side = u.get('side')
            price = float(u.get('price_level', 0))
            size = float(u.get('new_quantity', 0))
            book = self.bids if side == 'bid' else self.asks
            if size == 0:
                book.pop(price, None)
            else:
                book[price] = size
    def _imbalance(self):
        bid_vol = sum(s for _, s in self.bids)
        ask_vol = sum(s for _, s in self.asks)
        return bid_vol / (bid_vol + ask_vol) if (bid_vol + ask_vol) > 0 else None

    def _write_snapshot(self, now: datetime):
        if not self.output_file:
            return
        data = {
            "t": now.isoformat(),
            "p": self.product_id,
            "s": self.sequence_num,
            "bb": self.best_bid,
            "ba": self.best_ask,
            "sp": self.spread,
            "mp": self.mid_price,
            "ib": self.imbalance()
        }
        try:
            self.output_file.parent.mkdir(parents=True, exist_ok=True)
            with gzip.open(f"{self.output_file}.gz", "at", encoding='utf-8') as f:
                f.write(json.dumps(data) + '\n')
        except Exception as e:
            print(f"[OrderBookState] Failed to write metrics: {e}")
