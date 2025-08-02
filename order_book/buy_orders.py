import api_scripts.get_request as get_req
import api_scripts.post_requests as post_req
from decimal import Decimal, ROUND_DOWN

# ==== ROUND BASE SIZE ====
def round_base_size(value, increment):
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

buyOrder(usdc_amount = 1)