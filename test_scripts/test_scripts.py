import api_scripts.get_request as get_req
import api_scripts.post_requests as post_req
import numpy as np
from decimal import Decimal, ROUND_DOWN

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

#buyOrder(usdc_amount = 1)
# testPriceHistory()
# testCreateOrder()
testCurrentPrice()
# testOpenOrder()