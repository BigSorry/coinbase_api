import api_scripts.get_request as get_req
import api_scripts.post_requests as post_req
import numpy as np
from decimal import Decimal, ROUND_DOWN
import matplotlib.pyplot as plt
from collections import defaultdict

import util
from websocket_scripts.orderbook_analyze import OrderBookState

def testCurrentPrice():
    coin_pair = "ETH-USD"
    current_price = get_req.getCurrentPrice(coin_pair)
    print(coin_pair, current_price)


# Get a list of fills filtered by optional query parameters
def testOpenOrder():
    # Default all fills
    end_point_param = "orders/historical/fills"
    orders = get_req.getOrders(end_point_param)
    fills = orders["fills"]
    for fill in fills:
        print(fill["product_id"], fill["trade_time"])
        break

def testCreateOrder():
    # Default all fills
    post_req.createOrder()

def testPriceHistory():
    ids = ["BTC-USD", "ETH-USD"]
    days_ago = 30
    historical_data_df = get_req.getPriceHistory(ids, days_ago, granularity_unit=3600, df_return=True) #days
    print(historical_data_df.head())

# ==== ROUND BASE SIZE ====
def round_base_size(value, increment):
    precision = Decimal(increment).as_tuple().exponent * -1
    return Decimal(value).quantize(Decimal(increment), rounding=ROUND_DOWN)
def buyOrder(usdc_amount = 1):
    product_ids = ["BTC-USD", "ETH-USD", "XRP-USD", "SOL-USD",
                   "ADA-USD", "SUI-USD", "HBAR-USD"]
    prices = get_req.getCurrentBestBidAsk(product_ids)
    for product_id in product_ids:
        info = get_req.getCurrentPrice(product_id)
        if 'price' not in info:
            print(f"❌ Failed to fetch product info for {product_id}")
            continue

        best_bid_price = Decimal(prices[product_id]["bids"][0]["price"])
        base_increment = info['base_increment']
        min_order_size = Decimal(info['base_min_size'])

        # Calculate base size (amount of coin to buy)
        base_size = Decimal(usdc_amount) / best_bid_price
        base_size = round_base_size(base_size, base_increment)

        if base_size < min_order_size:
            print(f"⚠️ Order for {product_id} too small: {base_size} < min {min_order_size}")
            continue


        post_req.buyLimitOrder(product_id, str(best_bid_price), str(base_size))

# 👇 Grouping function
def group_orders(prices, sizes, bucket_size):
    grouped = defaultdict(float)
    for p, s in zip(prices, sizes):
        bucket = round(p / bucket_size) * bucket_size
        grouped[bucket] += s
    return zip(*sorted(grouped.items()))
def testOrderBooks(depth_limit=1000):
    # Parse order book
    # Read file (list of snapshots)
    snapshots = util.readZIP("../websocket_scripts/data/order_book_ETH-USD_2025-08-06T18-42-10.jsonl.gz")

    if not snapshots:
        print("No snapshots found.")
        return
    for i in range(len(snapshots)):
        # Use the latest snapshot (or choose based on index)
        latest_snapshot = snapshots[i]

        # Reconstruct OrderBookState from snapshot
        book = OrderBookState(
            timestamp=latest_snapshot["timestamp"],
            product_id=latest_snapshot["product_id"],
            sequence_num=latest_snapshot.get("sequence_num")
        )

        # Restore bids and asks
        for price, size in latest_snapshot["bids"]:
            book.bids[float(price)] = float(size)

        for price, size in latest_snapshot["asks"]:
            book.asks[float(price)] = float(size)

        # Get depth data
        bids, asks = book.get_depth_data(levels=depth_limit)
        bid_prices, bid_sizes = zip(*bids) if bids else ([], [])
        ask_prices, ask_sizes = zip(*asks) if asks else ([], [])

        # Cumulative sizes
        bid_cumsum = np.cumsum(bid_sizes)
        ask_cumsum = np.cumsum(ask_sizes)

        # Plot depth chart
        plt.figure(figsize=(12, 6))

        plt.step(bid_prices, bid_cumsum, where='post', label='Bids', color='green')
        plt.step(ask_prices, ask_cumsum, where='post', label='Asks', color='red')

        # Highlight best bid and ask
        plt.axvline(bid_prices[0], color='green', linestyle='--', alpha=0.4)
        plt.axvline(ask_prices[0], color='red', linestyle='--', alpha=0.4)

        plt.title("Order Book Depth Chart")
        plt.xlabel("Price (USD)")
        plt.ylabel("Cumulative Size")
        plt.legend()
        plt.grid(True, linestyle='--', alpha=0.5)
        plt.tight_layout()
    plt.show()


testOrderBooks()

#buyOrder(usdc_amount = 1)
# testPriceHistory()
# testCreateOrder()
#testCurrentPrice()
# testOpenOrder()