import api_scripts.get_request as get_req
import api_scripts.post_requests as post_req
from decimal import Decimal, ROUND_DOWN

# ==== ROUND BASE SIZE ====
from websocket_scripts.order_book_classes import FullOrderBookState


def round_base_size(value, increment):
    return Decimal(value).quantize(Decimal(increment), rounding=ROUND_DOWN)

def sellOrder(fiat_amount=1):
    product_ids = ["BTC-USD", "ETH-USD", "XRP-USD", "SOL-USD",
                   "ADA-USD"]
    prices = get_req.getCurrentBestBidAsk(product_ids)
    for product_id in product_ids:
        info = get_req.getProductInfo(product_id)
        if 'price' not in info:
            print(f"❌ Failed to fetch product info for {product_id}")
            continue

        best_ask_price = Decimal(prices[product_id]["asks"][0]["price"])
        base_increment = info['base_increment']
        min_order_size = Decimal(info['base_min_size'])
        # Calculate base size (amount of coin to sell)
        base_size = Decimal(fiat_amount) / best_ask_price
        base_size = round_base_size(base_size, base_increment)

        if base_size < min_order_size:
            print(f"⚠️ Order for {product_id} too small: {base_size} < min {min_order_size}")
            continue

        post_req.sellLimitOrder(product_id, str(best_ask_price), str(base_size))

def sellPortFolio(percentage_of_portfolio=0.1):
    portfolio_dict = get_req.getPortfolio(min_value_usdc=20, fiat_currency="USD")
    product_ids = list(portfolio_dict.keys())
    prices = get_req.getCurrentBestBidAsk(product_ids)
    for product_id, balance_amount in portfolio_dict.items():
        info = get_req.getProductInfo(product_id)
        if 'price' not in info:
            print(f"❌ Failed to fetch product info for {product_id}")
            continue
        crypto_selling = balance_amount * percentage_of_portfolio
        best_ask_price = Decimal(prices[product_id]["asks"][0]["price"])
        base_increment = info['base_increment']
        base_size = round_base_size(crypto_selling, base_increment)

        min_order_size = Decimal(info['base_min_size'])
        if base_size < min_order_size:
            print(f"⚠️ Order for {product_id} too small: {base_size} < min {min_order_size}")
            continue

        post_req.sellLimitOrder(product_id, str(best_ask_price), str(base_size))


from collections import defaultdict

def find_wall(order_book, side, price_window=0.05, wall_factor=10, tick_group=.10):
    bids, asks = order_book['bids'], order_book['asks']
    best_bid = float(bids[0][0])
    best_ask = float(asks[0][0])
    mid_price = (best_bid + best_ask) / 2

    if side == "buy":
        levels = [(float(price), float(s)) for price, s, _ in asks
                  if mid_price < float(price) <= mid_price * (1 + price_window)]
    else:  # side == "sell"
        levels = [(float(price), float(s)) for price, s, _ in bids
                  # Price between lower and upper bound
                  if mid_price * (1 - price_window) <= float(price) < mid_price]

    if not levels:
        return None

    # ---- Group into buckets ----
    grouped = defaultdict(float)
    for price, size in levels:
        bucket_price = round(price / tick_group) * tick_group
        grouped[bucket_price] += size

    # Average volume for threshold
    grouped_iters = list(grouped.items())
    cumulative_levels = [(price, sum(size for _, size in grouped_iters[:i + 1])) for i, (price, size) in
                         enumerate(grouped_iters)]

    avg_volume = sum(size for _, size in cumulative_levels) / len(grouped)
    threshold = wall_factor * avg_volume

    for price, cumm_size in cumulative_levels:
        if cumm_size >= threshold:
            return {
                "mid_price": mid_price,
                "wall_price": price,
                "cum_wall_size": cumm_size,
                "wall_total_value": price * cumm_size,
                "avg_volume": avg_volume
            }

    return None

def updateStopLimit(post_new_orders=False):
    portfolio_dict = get_req.getPortfolio(min_value_usdc=20, fiat_currency="USD", include_holds=True)

    for trade_pair_id, balance_amount in portfolio_dict.items():
        books = get_req.getOrderBook(trade_pair_id, detail_level=2)

        test = find_wall(books, "sell", price_window=0.1, wall_factor=1, tick_group=.01)
        stop_price = test["wall_price"]
        limit_price = round(stop_price * 0.99, 2)
        print(f"{trade_pair_id} Stop Price: {stop_price}, Limit Price: {limit_price}")
        if post_new_orders:
            post_req.placeStopLimitOrder(trade_pair_id, str(stop_price), str(limit_price),
                                     str(balance_amount), "SELL")
updateStopLimit()