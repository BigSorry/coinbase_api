import api_scripts.get_request as get_req
import api_scripts.post_requests as post_req
from decimal import Decimal, ROUND_DOWN

# ==== ROUND BASE SIZE ====
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

sellPortFolio(percentage_of_portfolio=.1)