import datetime
import pandas as pd
import api_scripts.get_request as prd_req


# Function to process the coins and calculate the adjusted quantities
def adjust_coin_quantities(coins, percentage_increase=1.05):
    result = {}

    for coin, fills in coins.items():
        # Aggregate the fills (sum up price * quantity for total cost, and sum the quantities)
        total_qty = sum(qty for _, qty in fills)
        total_cost = sum(price * qty for price, qty in fills)

        # Original price and original total worth
        original_avg_price = (
            total_cost / total_qty
        )  # Weighted average price for the coin

        # New price is 10% more than the original price
        new_price = original_avg_price * percentage_increase

        # New quantity to keep the total worth the same
        new_qty = total_cost / new_price
        sell_quant = total_qty - new_qty
        result[coin] = {
            "original_avg_price": original_avg_price,
            "total_qty": total_qty,
            "total_cost": total_cost,
            "sell_price": new_price,
            "sell_quant": sell_quant,
        }

    return result


def sellProfit(old_price, old_quant, price_increase_multi):
    sell_price = old_price * price_increase_multi
    # Calculate the new quantity to keep the total worth the same
    quant_new = old_quant / price_increase_multi
    sell_amount = old_quant - quant_new

    return sell_price, sell_amount


sel_date = datetime.datetime(2025, 6, 1, 0, 0)
#sel_date = datetime.datetime(2025, 6, 20, 0, 0)
rfc3339_timestamp = sel_date.strftime("%Y-%m-%dT%H:%M:%SZ")
params = f"?start_sequence_timestamp={rfc3339_timestamp}"
end_point_param = "orders/historical/fills"
end_point_param2 = f"orders/historical/fills{params}"
orders = prd_req.getOrders(end_point_param)
fills = orders["fills"]
buy_pair_dict = {}
for fill in fills:
    trade_time = fill["trade_time"]
    date = datetime.datetime.fromisoformat(trade_time[:-1])
    if date >= sel_date:
        side = fill["side"]
        if side.upper() == "BUY":
            prd_id = fill["product_id"]
            trade_price = float(fill["price"])
            trade_quantity = float(fill["size"])
            buy_pair = (trade_price, trade_quantity)
            if prd_id not in buy_pair_dict:
                buy_pair_dict[prd_id] = [buy_pair]
            else:
                buy_pair_dict[prd_id].append(buy_pair)

price_increase = 1
result = adjust_coin_quantities(buy_pair_dict, percentage_increase=price_increase)
for coin_pair, buy_info in result.items():
    endpoint = f"/api/v3/brokerage/products/{coin_pair}/"
    price_dict = prd_req.getApiAdvanced(endpoint)
    current_price = float(price_dict["price"])
    price_increase = (current_price / buy_info["original_avg_price"]) * 100
    print(coin_pair)
    print(f"calc with {price_increase:.3f}")
    sold_total = buy_info["sell_price"] * buy_info["sell_quant"]
    print(buy_info["original_avg_price"], "old vs new", current_price)
    print()
