import datetime
import pandas as pd
import api_scripts.get_request as api_get
from collections import defaultdict
def getBuyOrders(all_orders, sel_date, sel_status="FILLED",
                 sel_side="BUY",sel_order_type="LIMIT"):
    buy_pair_dict = defaultdict(dict)

    for order in all_orders:
        trade_time = order.get('created_time', '')
        date = datetime.datetime.fromisoformat(trade_time[:-1])
        if date < sel_date:
            continue

        side = order.get('side', '').upper()
        status = order.get('status', '').upper()
        if side != sel_side or status != sel_status:
            continue

        order_type = order.get('order_type', '').upper()
        if sel_order_type in order_type:
            product_id = order.get('product_id', "")
            order_value = float(order.get('total_value_after_fees', 0))
            avg_price = float(order.get('average_filled_price', 0))
            total_qty = float(order.get('filled_size', 0))
            buy_pair_dict[product_id] = {
                "avg_price": avg_price,
                "total_qty": total_qty,
                "order_value": order_value,
            }

    return buy_pair_dict

sel_date = datetime.datetime(2025, 7, 1, 0, 0)
get_endpoint = f"orders/historical/batch"
orders = api_get.getOrders(get_endpoint)["orders"]

buy_orders_dict = getBuyOrders(orders, sel_date, sel_status="FILLED",
                                sel_side="BUY",sel_order_type="LIMIT")
sold_orders_dict = getBuyOrders(orders, sel_date, sel_status="OPEN",
                                sel_side="SELL",sel_order_type="LIMIT")
fee_multiplier = 0.9975 # 0.25% sell fee
for coin_pair, buy_info in buy_orders_dict.items():
    current_price = api_get.getCurrentPrice(coin_pair)
    fee_reduced_price = current_price * fee_multiplier
    price_percent_change = (fee_reduced_price / buy_info["avg_price"]) * 100

    print(coin_pair)
    print(f"calc with {price_percent_change:.3f}")
    print(buy_info["avg_price"], "old vs new", fee_reduced_price)
    print()
