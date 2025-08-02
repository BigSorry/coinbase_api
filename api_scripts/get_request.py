import json,requests
import datetime
import api_scripts.authenticate as auth
import pandas as pd
"""
Method below are using a base url for the advanced coinbase api
"""
def getApiAdvanced(endpoint):
    "Fetches the latest price for a given product ID from Coinbase Advanced Trade API."
    request_method = "GET"
    request_host = "api.coinbase.com"
    jwt_token = auth.getJWT(request_method, request_host, endpoint)

    headers = {
        "Authorization": f"Bearer {jwt_token}",
        "Content-Type": "application/json"
    }
    base_url = "https://api.coinbase.com"
    url = base_url + endpoint
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        return data
    else:
        print(f"Error: {response.status_code}, {response.text}")
        return None

def getPortfolio(min_value_usdc=50, fiat_currency="USD"):
    "Fetches the latest price for a given product ID from Coinbase Advanced Trade API."
    endpoint = f"/api/v3/brokerage/accounts"
    portfolio_data = getApiAdvanced(endpoint)
    items_included = {}
    fiats = ["USD", "USDC", "EUR", "GBP", "JPY", "AUD", "CAD"]
    for account in portfolio_data.get('accounts', []):
        balance = float(account['available_balance']['value'])
        if balance is None or balance <= 0:
            continue
        currency = account["currency"]
        if any(fiat in currency for fiat in fiats):
            # Skip fiat currencies
            continue
        product_id = f"{currency}-{fiat_currency}"
        current_price = getCurrentPrice(product_id)
        total_value = balance * current_price if current_price else 0
        if total_value > min_value_usdc:
            items_included[product_id] = balance
    return items_included
def getProductInfo(product_id):
    "Get Product details "
    endpoint = f"/api/v3/brokerage/products/{product_id}"
    return getApiAdvanced(endpoint)
def getCurrentPrice(product_id):
    "Fetches the latest price for a given product ID from Coinbase Advanced Trade API."
    endpoint = f"/api/v3/brokerage/products/{product_id}/ticker"
    price_dict = getApiAdvanced(endpoint)
    bid_price = price_dict["best_bid"]
    ask_price = price_dict["best_ask"]
    if not bid_price or not ask_price:
        spot_price = price_dict['trades'][0]['price']
    else:
        spot_price = (float(bid_price) + float(ask_price)) / 2
    return spot_price
def getCurrentBestBidAsk(product_ids):
    "Fetches the latest price for a given product ID from Coinbase Advanced Trade API."
    product_list = ','.join(product_ids)
    endpoint = f"/api/v3/brokerage/best_bid_ask"
    all_prices = getApiAdvanced(endpoint)
    # filter out what we need, this works without getting 401
    result = {}
    for dict_info in all_prices["pricebooks"]:
        product_id = dict_info['product_id']
        if product_id in product_ids:
            result[product_id] = dict_info
    return result

def getOrders(end_point_param):
    """Fetches open orders from Coinbase Advanced Trade API."""
    request_method = "GET"
    request_host = "api.coinbase.com"
    endpoint = f"/api/v3/brokerage/{end_point_param}"
    return getApiAdvanced(endpoint)

"""
First method made for coinbase base exchange
"""
def getAPIData(base_url, product_ids, url_param, headers=None):
    """
    General function to fetch data from a specified API endpoint for a given product ID and info category.

    :param base_url: The URL template with placeholders for product ID and info category,
                              e.g., 'https://api.exchange.coinbase.com/products/{}/{}'
    :param product_id: The product ID to be inserted into the URL
    :param url_param: Last part of the url with parameters
     Often the category of information to fetch (e.g., 'stats', 'ticker', 'orderbook')
    :param headers: Optional headers to include in the request
    :return: The fetched data as a dictionary, or None if the request fails
    """
    # Ensure input is a list, even if a single ID is provided
    is_single_id = isinstance(product_ids, str)
    if is_single_id:  # Single ID
        product_ids = [product_ids]

    headers = headers or {'Accept': 'application/json'}
    results = {}
    for product_id in product_ids:
        url = base_url.format(product_id, url_param)
        response = requests.get(url, headers=headers)

        if response.status_code == 200:  # Only process successful responses
            try:
                data = response.json()
                if data:  # Check if the response contains non-empty data
                    results[product_id] = data
            except json.JSONDecodeError:
                print(f"Failed to parse JSON for ID {product_id}")
        else:
            print(f"Request failed with status code {response.status_code} for ID {product_id}, {response.content}")

    return results if not is_single_id else results.get(product_ids[0])


def getOrderBook(product_id="BTC-USD", detail_level=2):
    base_url = 'https://api.exchange.coinbase.com/products/{}/{}'
    # level three gets entire order book
    url_param = f"book?level={detail_level}"
    # return dict with key id and values per timestamp
    order_books = getAPIData(base_url, product_id, url_param)

    return order_books


def getPriceHistory(coin_pair_ids, days_ago, granularity_unit=3600, df_return=False):
    days_ago_limit = min(12, days_ago) # max 300 candles so actually depends on our granularity unit
    timestamp_start = datetime.datetime.now() - pd.DateOffset(days=days_ago_limit)
    timestamp_end = datetime.datetime.now()
    url_param = f"candles?granularity={granularity_unit}&start={timestamp_start}&end={timestamp_end}"
    base_url = 'https://api.exchange.coinbase.com/products/{}/{}'
    # return dict with key id and values per timestamp
    historical_data = getAPIData(base_url, coin_pair_ids, url_param)
    if df_return:
        historical_data = convertDF(historical_data)

    return historical_data

def convertDF(dict_data):
    # Convert to a list of rows
    rows = []
    for pair, timestamp_values in dict_data.items():
        for values in timestamp_values:
            timestamp, open_, high, low, close, volume = values
            dt = datetime.datetime.utcfromtimestamp(timestamp)  # Convert to readable date
            rows.append([pair, dt, open_, high, low, close, volume])
    # Create DataFrame
    return pd.DataFrame(rows, columns=["pair", "timestamp", "open", "high", "low", "close", "volume"])