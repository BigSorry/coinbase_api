import datetime
import pandas as pd
import api_scripts.get_request as api_get
from collections import defaultdict
def getBuyFills(fills, sel_date):
    buy_pair_dict = defaultdict(list)

    for fill in fills:
        # Parse trade time and filter by date
        trade_time = fill["trade_time"]
        date = datetime.datetime.fromisoformat(trade_time[:-1])
        if date < sel_date:
            continue

        # Check if the side is "BUY"
        if fill["side"].upper() != "BUY":
            continue

        # Extract product ID, price, and quantity
        prd_id = fill["product_id"]
        trade_price = float(fill["price"])
        trade_quantity = float(fill["size"])
        fee = float(fill.get("commission", 0))
        buy_pair = (trade_price, trade_quantity, fee)

        # Use setdefault to simplify dictionary updates
        buy_pair_dict[prd_id].append(buy_pair)

    return buy_pair_dict

# Function to process the coins and calculate the adjusted quantities
def adjust_coin_quantities(fills):
    result = {}

    for coin_pair_id, list_fills in fills.items():
        # Aggregate the fills
        total_qty = sum(qty for _, qty, _ in list_fills)
        total_cost = sum((price * qty) + fee for price, qty, fee in list_fills)
        # Average price among the fills
        original_avg_price = (
            total_cost / total_qty
        )  # Weighted average price for the coin

        result[coin_pair_id] = {
            "avg_price": original_avg_price,
            "total_qty": total_qty,
            "total_cost": total_cost,
        }

    return result

sel_date = datetime.datetime(2025, 8, 1, 0, 0)
end_point_param = "orders/historical/fills"
orders = api_get.getOrders(end_point_param)
fills = orders["fills"]
buy_fills_dict = getBuyFills(fills, sel_date)
result = adjust_coin_quantities(buy_fills_dict)

for coin_pair, buy_info in result.items():
    current_price = api_get.getCurrentPrice(coin_pair)
    price_percent_change = (current_price / buy_info["avg_price"]) * 100

    print(coin_pair)
    print(f"calc with {price_percent_change:.3f}")
    print(buy_info["avg_price"], "old vs new", current_price)
    print()
