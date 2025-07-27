import api_scripts.get_request as get_req
import api_scripts.post_requests as post_req
import numpy as np

def testCurrentPrice():
    coin_pair = "BTC-USD"
    price_dict = get_req.getCurrentPrice(coin_pair)
    current_price = float(price_dict["price"])
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

testPriceHistory()
testCreateOrder()
testCurrentPrice()
testOpenOrder()