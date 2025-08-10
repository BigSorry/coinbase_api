from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Tuple, Optional
from pathlib import Path
import communication as comm
import gzip
import json

@dataclass
class PriceHistoryTracker:
    product_id: str = "" # trade pair
    min_change_pct: float = 0.001  # 0.1% default
    big_change_pct: float = 0.05
    min_change_abs: float = 0.0    # absolute price change filter
    min_time_interval: float = 5.0 # seconds
    write_interval: int = 30
    last_write_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    max_size: [int] = 100
    history: List[Tuple[datetime, float]] = field(default_factory=list)
    _last_time: Optional[datetime] = None
    _last_price: Optional[float] = None
    output_file: Optional[Path] = None

    def record(self, price: Optional[float]):
        """Record price if change thresholds are exceeded."""
        if price is None:
            return

        now = datetime.now(timezone.utc)

        if self._last_time is None:
            self._append(now, price)
            return

        time_diff = (now - self._last_time).total_seconds()
        if time_diff < self.min_time_interval:
            return

        pct_change = abs(price - self._last_price) / self._last_price if self._last_price else 0
        abs_change = abs(price - self._last_price)

        if pct_change >= self.big_change_pct:
            comm.send_mail(self.product_id, self.history)
        if pct_change >= self.min_change_pct or abs_change >= self.min_change_abs:
            self._append(now, price)

    def _append(self, now: datetime, price: float):
        self.history.append((now, price))
        self._last_time = now
        self._last_price = price
        if self.max_size and len(self.history) > self.max_size:
            self.history.pop(0)

    def write_prices(self):
        now = datetime.now(timezone.utc)
        if (now - self.last_write_time).total_seconds() >= self.write_interval:
            self.last_write_time = now
            price_history = {
                "timestamp": now.isoformat(),
                "product_id": self.product_id,
                "times": [date_obj.isoformat() for date_obj, _ in self.history],
                "prices": [price for _, price in self.history],
                "last_price": self._last_price,
                "last_time": self._last_time.isoformat()
            }
            try:
                self.output_file.parent.mkdir(parents=True, exist_ok=True)
                with gzip.open(f"{self.output_file}.gz", "at", encoding='utf-8') as f:
                    f.write(json.dumps(price_history) + '\n')
            except Exception as e:
                print(f"[OrderBookState] Failed to write metrics: {e}")

