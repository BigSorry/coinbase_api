"""
Order Book Data Analyzer with Depth Charts
==========================================

This module reads the WebSocket output from the Coinbase tracker and provides:
- Data parsing and cleaning
- Order book reconstruction
- Depth chart visualization
- Spread analysis
- Volume analysis
- Market microstructure metrics
- Real-time and historical analysis

Requirements:
pip install pandas matplotlib seaborn plotly numpy scipy
"""

import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass, field
from pathlib import Path
import logging
from collections import defaultdict, deque
import bisect
from scipy import stats

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class OrderBookState:
    """Complete order book state at a point in time"""
    timestamp: str
    product_id: str
    bids: Dict[float, float] = field(default_factory=dict)  # price -> size
    asks: Dict[float, float] = field(default_factory=dict)  # price -> size
    sequence_num: Optional[int] = None

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


class OrderBookAnalyzer:
    """
    Comprehensive order book data analyzer with visualization capabilities
    """

    def __init__(self, data_file: str):
        self.data_file = Path(data_file)
        self.raw_data: List[Dict] = []
        self.order_books: Dict[str, OrderBookState] = {}  # product_id -> latest state
        self.historical_states: Dict[str, List[OrderBookState]] = defaultdict(list)
        self.metrics_history: Dict[str, List[Dict]] = defaultdict(list)

        if not self.data_file.exists():
            raise FileNotFoundError(f"Data file not found: {data_file}")

    def load_data(self, limit: Optional[int] = None) -> None:
        """Load and parse the WebSocket data"""
        logger.info(f"Loading data from {self.data_file}")

        with open(self.data_file, 'r') as f:
            for i, line in enumerate(f):
                if limit and i >= limit:
                    break

                try:
                    data = json.loads(line.strip())
                    self.raw_data.append(data)
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse line {i + 1}: {e}")

        logger.info(f"Loaded {len(self.raw_data)} messages")
        self._process_messages()

    def _process_messages(self) -> None:
        """Process raw messages into order book states"""
        logger.info("Processing messages into order book states...")

        for msg in self.raw_data:
            events = msg.get('events', [])[0]
            msg_type = events.get('type')
            product_id = events.get('product_id', 'UNKNOWN')

            if msg_type == 'snapshot':
                self._process_snapshot(msg, product_id)
            elif msg_type == 'update':
                self._process_l2_update(msg, product_id)

    def _process_snapshot(self, msg: Dict, product_id: str) -> None:
        """Process snapshot message"""
        timestamp = msg.get('received_at', msg.get('timestamp', ''))

        # Create new order book state
        order_book = OrderBookState(
            timestamp=timestamp,
            product_id=product_id,
            sequence_num=msg.get('sequence_num')
        )
        update_list = msg.get('events', [])[0]['updates']
        # Parse bids and asks
        for update_item in update_list:
            side = update_item.get('side')
            price = float(update_item.get('price_level', 0))
            size = float(update_item.get('new_quantity' , 0))
            if size > 0:
                if side == 'bid':
                    order_book.bids[price] = size
                elif side == 'offer':
                    order_book.asks[price] = size

        # Update current state and history
        self.order_books[product_id] = order_book
        self.historical_states[product_id].append(order_book)
        self._calculate_metrics(order_book)

    def _process_l2_update(self, msg: Dict, product_id: str) -> None:
        """Process L2 update message"""
        if product_id not in self.order_books:
            logger.warning(f"Received update for unknown product: {product_id}")
            return

        # Get current state and create updated copy
        current_state = self.order_books[product_id]
        updated_state = OrderBookState(
            timestamp=msg.get('received_at', msg.get('time', '')),
            product_id=product_id,
            bids=current_state.bids.copy(),
            asks=current_state.asks.copy(),
            sequence_num=msg.get('sequence_num')
        )

        # Apply updates
        update_list = msg.get('events', [])[0]['updates']
        for update_item in update_list:
            side = update_item.get('side')
            price = float(update_item.get('price_level', 0))
            size = float(update_item.get('new_quantity', 0))

            if side == 'bid':
                if size == 0:
                    updated_state.bids.pop(price, None)
                else:
                    updated_state.bids[price] = size
            elif side == 'offer':
                if size == 0:
                    updated_state.asks.pop(price, None)
                else:
                    updated_state.asks[price] = size

        # Update current state and history
        self.order_books[product_id] = updated_state
        self.historical_states[product_id].append(updated_state)
        self._calculate_metrics(updated_state)

    def _calculate_metrics(self, order_book: OrderBookState) -> None:
        """Calculate various order book metrics"""
        metrics = {
            'timestamp': order_book.timestamp,
            'product_id': order_book.product_id,
            'best_bid': order_book.best_bid,
            'best_ask': order_book.best_ask,
            'spread': order_book.spread,
            'mid_price': order_book.mid_price,
            'bid_levels': len(order_book.bids),
            'ask_levels': len(order_book.asks),
            'total_bid_volume': sum(order_book.bids.values()),
            'total_ask_volume': sum(order_book.asks.values()),
        }

        # Calculate depth metrics
        if order_book.bids and order_book.asks:
            metrics.update(self._calculate_depth_metrics(order_book))

        self.metrics_history[order_book.product_id].append(metrics)

    def _calculate_depth_metrics(self, order_book: OrderBookState, depth_levels: List[float] = None) -> Dict:
        """Calculate depth metrics at various price levels"""
        if depth_levels is None:
            depth_levels = [0.1, 0.5, 1.0, 2.0, 5.0]  # Percentage from mid price

        metrics = {}
        mid_price = order_book.mid_price

        if not mid_price:
            return metrics

        for depth_pct in depth_levels:
            depth_price = mid_price * (depth_pct / 100)

            # Calculate bid depth
            bid_volume = sum(
                size for price, size in order_book.bids.items()
                if price >= mid_price - depth_price
            )

            # Calculate ask depth
            ask_volume = sum(
                size for price, size in order_book.asks.items()
                if price <= mid_price + depth_price
            )

            metrics[f'bid_depth_{depth_pct}pct'] = bid_volume
            metrics[f'ask_depth_{depth_pct}pct'] = ask_volume
            metrics[f'total_depth_{depth_pct}pct'] = bid_volume + ask_volume
            metrics[f'imbalance_{depth_pct}pct'] = (bid_volume - ask_volume) / (bid_volume + ask_volume) if (
                                                                                                                        bid_volume + ask_volume) > 0 else 0

        return metrics

    def create_depth_chart(self, product_id: str, timestamp: Optional[str] = None,
                           levels: int = 500, interactive: bool = True) -> None:
        """Create a depth chart visualization"""
        if product_id not in self.order_books:
            raise ValueError(f"No data available for product: {product_id}")

        # Get the order book state
        if timestamp:
            # Find specific timestamp (simplified - would need more sophisticated search)
            order_book = self.order_books[product_id]  # For now, use latest
        else:
            order_book = self.order_books[product_id]

        bids, asks = order_book.get_depth_data(levels)

        if interactive:
            self._create_plotly_depth_chart(order_book, bids, asks)

    def _create_plotly_depth_chart(self, order_book: OrderBookState,
                                   bids: List[Tuple[float, float]],
                                   asks: List[Tuple[float, float]]) -> None:
        """Create interactive depth chart using Plotly"""
        # Prepare data for cumulative depth
        bid_prices, bid_sizes = zip(*bids) if bids else ([], [])
        ask_prices, ask_sizes = zip(*asks) if asks else ([], [])

        # Calculate cumulative volumes
        bid_cumulative = np.cumsum(bid_sizes) #[::-1])[::-1]  # Reverse for proper cumulative
        ask_cumulative = np.cumsum(ask_sizes)

        # Create figure with secondary y-axis
        fig = make_subplots(
            rows=2, cols=1,
            subplot_titles=(f'Order Book Depth - {order_book.product_id}', 'Order Book Levels'),
            vertical_spacing=0.1,
            row_heights=[0.7, 0.3]
        )

        # Depth chart (cumulative)
        if bid_prices:
            fig.add_trace(
                go.Scatter(
                    x=list(bid_prices),
                    y=list(bid_cumulative),
                    fill='tonexty',
                    mode='lines',
                    name='Bids (Cumulative)',
                    line=dict(color='green', width=2),
                    fillcolor='rgba(0, 255, 0, 0.3)'
                ),
                row=1, col=1
            )

        if ask_prices:
            fig.add_trace(
                go.Scatter(
                    x=list(ask_prices),
                    y=list(ask_cumulative),
                    fill='tozeroy',
                    mode='lines',
                    name='Asks (Cumulative)',
                    line=dict(color='red', width=2),
                    fillcolor='rgba(255, 0, 0, 0.3)'
                ),
                row=1, col=1
            )

        # Individual levels bar chart
        if bid_prices:
            fig.add_trace(
                go.Bar(
                    x=list(bid_prices),
                    y=list(bid_sizes),
                    name='Bid Levels',
                    marker_color='green',
                    opacity=0.7
                ),
                row=2, col=1
            )

        if ask_prices:
            fig.add_trace(
                go.Bar(
                    x=list(ask_prices),
                    y=list(ask_sizes),
                    name='Ask Levels',
                    marker_color='red',
                    opacity=0.7
                ),
                row=2, col=1
            )

        # Add vertical line for mid price
        if order_book.mid_price:
            fig.add_vline(
                x=order_book.mid_price,
                line_dash="dash",
                line_color="black",
                annotation_text=f"Mid: ${order_book.mid_price:.2f}"
            )

        # Update layout
        fig.update_layout(
            title=f"Order Book Analysis - {order_book.product_id}<br>"
                  f"Spread: ${order_book.spread:.2f} | Mid: ${order_book.mid_price:.2f}",
            xaxis_title="Price ($)",
            yaxis_title="Cumulative Volume",
            height=800,
            showlegend=True,
            hovermode='x unified'
        )

        fig.update_xaxes(title_text="Price ($)", row=2, col=1)
        fig.update_yaxes(title_text="Volume", row=2, col=1)

        fig.show()

    def get_market_summary(self, product_id: str) -> Dict[str, Any]:
        """Get comprehensive market summary"""
        if product_id not in self.order_books:
            raise ValueError(f"No data available for product: {product_id}")

        order_book = self.order_books[product_id]
        metrics_df = pd.DataFrame(self.metrics_history[product_id])

        summary = {
            'product_id': product_id,
            'last_update': order_book.timestamp,
            'current_state': {
                'best_bid': order_book.best_bid,
                'best_ask': order_book.best_ask,
                'spread': order_book.spread,
                'mid_price': order_book.mid_price,
                'bid_levels': len(order_book.bids),
                'ask_levels': len(order_book.asks),
            },
            'statistics': {
                'avg_spread': metrics_df['spread'].mean(),
                'std_spread': metrics_df['spread'].std(),
                'min_spread': metrics_df['spread'].min(),
                'max_spread': metrics_df['spread'].max(),
                'avg_bid_levels': metrics_df['bid_levels'].mean(),
                'avg_ask_levels': metrics_df['ask_levels'].mean(),
            },
            'total_updates': len(self.historical_states[product_id])
        }

        return summary


def main():
    """Example usage of the OrderBookAnalyzer"""
    # Initialize analyzer
    analyzer = OrderBookAnalyzer("../data/websocket/coinbase_orderbook.jsonl")

    # Load data
    analyzer.load_data(limit=1000)  # Limit for testing

    # Get available products
    products = list(analyzer.order_books.keys())
    print(f"Available products: {products}")

    if products:
        product = products[0]

        # Create depth chart
        print(f"\nCreating depth chart for {product}...")
        analyzer.create_depth_chart(product, interactive=True)

        # Get market summary
        summary = analyzer.get_market_summary(product)
        print(f"\nMarket Summary for {product}:")
        print(json.dumps(summary, indent=2, default=str))


if __name__ == "__main__":
    main()