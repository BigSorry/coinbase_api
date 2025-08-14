import api_scripts.get_request as get_req
import api_scripts.post_requests as post_req
from decimal import Decimal, ROUND_DOWN

def roundingAmount(product_info, balance_amount):
    base_increment = product_info['base_increment']
    rounded_base_size = Decimal(balance_amount).quantize(Decimal(base_increment), rounding=ROUND_DOWN)
    return rounded_base_size

def sellPortFolio(percentage_of_portfolio=0.1):
    portfolio_dict = get_req.getPortfolio(min_value_usdc=20, fiat_currency="USD")
    product_ids = list(portfolio_dict.keys())
    prices = get_req.getCurrentBestBidAsk(product_ids)
    for product_id, balance_amount in portfolio_dict.items():
        product_info = get_req.getProductInfo(product_id)
        if 'price' not in product_info:
            print(f"❌ Failed to fetch product info for {product_id}")
            continue
        best_ask_price = Decimal(prices[product_id]["asks"][0]["price"])

        sell_amount = balance_amount * percentage_of_portfolio
        sell_size = roundingAmount(product_info, sell_amount)
        min_order_size = Decimal(product_info['base_min_size'])
        if sell_size < min_order_size:
            print(f"⚠️ Order for {product_id} too small: {sell_size} < min {min_order_size}")
            continue

        post_req.sellLimitOrder(product_id, str(best_ask_price), str(sell_size))


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
        # Find prices based on the order book walls
        test = find_wall(books, "sell", price_window=0.1, wall_factor=1, tick_group=.01)
        stop_price = test["wall_price"]
        limit_price = round(stop_price * 0.99, 2)
        # Round if there are too many decimals
        # Amount quantities allowed is also different per trade-pair
        product_info = get_req.getProductInfo(trade_pair_id)
        sell_size = roundingAmount(product_info, balance_amount)
        min_order_size = Decimal(product_info['base_min_size'])
        print(f"{trade_pair_id} Stop Price: {stop_price}, Limit Price: {limit_price}, sell size {sell_size}")
        if sell_size < min_order_size:
            print(f"⚠️ Order for {trade_pair_id} too small: {sell_size} < min {min_order_size}")
            continue

        if post_new_orders:
            post_req.placeStopLimitOrder(trade_pair_id, str(stop_price), str(limit_price),
                                     str(sell_size), "SELL")
updateStopLimit(post_new_orders=True)