import json, requests
import api_scripts.authenticate as auth
import uuid
from api_scripts.get_request import getApiAdvanced

def postApiAdvanced(endpoint, body_content):
    "Fetches the latest price for a given product ID from Coinbase Advanced Trade API."
    request_method = "POST"
    request_host = "api.coinbase.com"
    jwt_token = auth.postJWT(request_method, request_host, endpoint, body_content)

    headers = {
        "Authorization": f"Bearer {jwt_token}",
        "Content-Type": "application/json"
    }
    base_url = "https://api.coinbase.com"
    url = base_url + endpoint
    response = requests.post(url, headers=headers, json=body_content)

    # === PRINT RESULT ===
    print("Status Code:", response.status_code)
    try:
        print(json.dumps(response.json(), indent=2))
    except:
        print(response.text)

def createOrder():
    endpoint = f"/api/v3/brokerage/orders"
    # ==== Create order body ====
    order_payload = {
        "client_order_id": str(uuid.uuid4()),
        "product_id": "BTC-USDC",
        "side": "BUY",
        "order_configuration": {
            "limit_limit_gtc": {
                "base_size": "0.000001",
                "limit_price": "40000",
                "post_only": False
            }
        }
    }

    postApiAdvanced(endpoint, order_payload)

def placeLimitOrder(pair_id, limit_price, base_size, side):
    endpoint = f"/api/v3/brokerage/orders"
    # ==== Create order body ====
    order_payload = {
        "client_order_id": str(uuid.uuid4()),
        "product_id": pair_id + "C",
        "side": side,
        "order_configuration": {
            "limit_limit_gtc": {
                "base_size": base_size,
                "limit_price": limit_price,
                "post_only": True
            }
        }
    }

    postApiAdvanced(endpoint, order_payload)


def placeStopLimitOrder(pair_id, stop_price, limit_price, base_size, side):
    endpoint = f"/api/v3/brokerage/orders"
    # ==== Create order body ====
    order_payload = {
        "client_order_id": str(uuid.uuid4()),
        "product_id": pair_id + "C",
        "side": side,
        "order_configuration": {
            "stop_limit_stop_limit_gtc": {
                "stop_price": stop_price,
                "limit_price": limit_price,
                "base_size": base_size
            }
        }
    }

    postApiAdvanced(endpoint, order_payload)
def buyLimitOrder(pair_id, limit_price, base_size):
    placeLimitOrder(pair_id, limit_price, base_size, "BUY")

def sellLimitOrder(pair_id, limit_price, base_size):
    placeLimitOrder(pair_id, limit_price, base_size, "SELL")

def cancelOrder(cancel_side="SELL",cancel_order_type="STOP_LIMIT"):
    "Fetches the latest price for a given product ID from Coinbase Advanced Trade API."
    get_endpoint = f"/api/v3/brokerage/orders/historical/batch"
    post_endpoint = f"/api/v3/brokerage/orders/batch_cancel"
    delete_payload = {"order_ids": []}
    portfolio_data = getApiAdvanced(get_endpoint)
    orders = portfolio_data.get('orders', [])
    for order in orders:
        side = order.get('side', '').upper()
        status = order.get('status', '').upper()
        if side != cancel_side or status != "OPEN":
            continue

        order_type = order.get('order_type', '').upper()
        if order_type == cancel_order_type:
            client_order_id = order.get('order_id', "")
            product_id = order.get('product_id', "")
            if client_order_id and "BTC" in product_id:
                print("Try to delete:", product_id)
                delete_payload["order_ids"].append(client_order_id)

    postApiAdvanced(post_endpoint, delete_payload)


cancelOrder()