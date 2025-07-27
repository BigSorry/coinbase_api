import time
import datetime

# Replace with your Coinbase Pro API keys
API_KEY = 'YOUR_API_KEY'
API_SECRET = 'YOUR_API_SECRET'
PASSPHRASE = 'YOUR_API_PASSPHRASE'

client = cbpro.AuthenticatedClient(API_KEY, API_SECRET, PASSPHRASE)

PRODUCT_ID = 'BTC-USD'
MONTHLY_BUDGET = 1000  # USD
NUM_BUYS = 4  # Number of DCA buys in the month
BUY_WINDOW_DAYS = 2  # Days to watch price around scheduled buy date
CHECK_INTERVAL_SECONDS = 1800  # Check price every 30 minutes

# Target discount threshold to place limit order (e.g., 3% below last reference price)
TARGET_DIP_PERCENT = 0.03

def get_current_price():
    ticker = client.get_product_ticker(product_id=PRODUCT_ID)
    return float(ticker['price'])

def place_limit_buy_order(price, size):
    print(f"Placing limit buy order at {price} for size {size}")
    order = client.place_order(
        product_id=PRODUCT_ID,
        side='buy',
        order_type='limit',
        price=str(price),
        size=str(size),
        time_in_force='GTT',  # Good till time (can specify duration if needed)
        cancel_after='day'  # Cancel if not filled within a day
    )
    return order

def cancel_order(order_id):
    print(f"Cancelling order {order_id}")
    client.cancel_order(order_id)

def place_market_buy_order(size):
    print(f"Placing market buy order for size {size}")
    order = client.place_market_order(
        product_id=PRODUCT_ID,
        side='buy',
        size=str(size)
    )
    return order

def wait_for_fill(order_id, timeout_seconds):
    # Simple poll for order status until filled or timeout
    start = time.time()
    while time.time() - start < timeout_seconds:
        order = client.get_order(order_id)
        if order['status'] == 'done':
            print(f"Order {order_id} filled")
            return True
        time.sleep(30)
    print(f"Order {order_id} not filled within timeout")
    return False

def run_dca_strategy():
    buy_size_usd = MONTHLY_BUDGET / NUM_BUYS
    last_reference_price = get_current_price()
    print(f"Starting DCA with last_reference_price = {last_reference_price}")

    # Calculate scheduled buy dates for the month (evenly spaced)
    today = datetime.date.today()
    days_in_month = 30
    interval_days = days_in_month // NUM_BUYS
    buy_dates = [today + datetime.timedelta(days=i * interval_days) for i in range(NUM_BUYS)]

    for buy_date in buy_dates:
        window_start = buy_date - datetime.timedelta(days=BUY_WINDOW_DAYS//2)
        window_end = buy_date + datetime.timedelta(days=BUY_WINDOW_DAYS//2)

        print(f"Monitoring buy window from {window_start} to {window_end}")

        order_placed = False
        order_id = None

        # Keep checking price within buy window
        while datetime.date.today() <= window_end:
            current_price = get_current_price()
            print(f"Current BTC price: {current_price} USD")

            target_price = last_reference_price * (1 - TARGET_DIP_PERCENT)
            print(f"Target limit price for buy: {target_price} USD")

            if not order_placed and current_price <= target_price:
                size_btc = buy_size_usd / current_price
                order = place_limit_buy_order(price=target_price, size=size_btc)
                order_id = order.get('id')
                order_placed = True

            # If limit order placed, wait and check if filled
            if order_placed:
                filled = wait_for_fill(order_id, timeout_seconds=60*60*24)  # wait max 24h
                if filled:
                    last_reference_price = target_price
                    break
                else:
                    cancel_order(order_id)
                    order_placed = False

            time.sleep(CHECK_INTERVAL_SECONDS)

        # If order never placed or never filled, buy at market price at end of window
        if not order_placed:
            current_price = get_current_price()
            size_btc = buy_size_usd / current_price
            place_market_buy_order(size=size_btc)
            last_reference_price = current_price

        print(f"Completed buy for window ending {window_end}\n")

    print("All DCA buys for the month completed!")

if __name__ == '__main__':
    run_dca_strategy()
