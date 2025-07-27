import time
import datetime
import util
import api_scripts.get_request as prd_req
import pandas as pd
import numpy as np

def main():
    ids_path = "data/coin_pairs_vol_100m_30days.pkl"
    ids_list = util.readPickle(ids_path)
    granularity_days = 3600
    timestamp_start = datetime.datetime.now() - pd.DateOffset(days=7)
    timestamp_end = timestamp_start + pd.DateOffset(days=1)
    not_included = []
    print(timestamp_start)
    print(timestamp_start)
    for prd_id in ids_list:
        if "USD" in prd_id:
            price_dict = prd_req.getCurrentPrice(prd_id)
            current_price = float(price_dict["price"])
            percentage_change_24h = 0
            if price_dict["price_percentage_change_24h"] != "":
                percentage_change_24h = float(price_dict["price_percentage_change_24h"])
            base_url = 'https://api.exchange.coinbase.com/products/{}/{}'
            url_param = f"candles?granularity={granularity_days}&start={timestamp_start}&end={timestamp_end}"

            historical_data = prd_req.getAPIData(base_url, prd_id, url_param)
            if historical_data:
                avg_price = np.mean(historical_data[0][1:-1])
                percentage_change = (current_price / avg_price)
                if percentage_change < 1.2:
                    print(f"{prd_id} with {percentage_change:2f} change since {timestamp_start} "
                          f"and 24hour change today {(percentage_change_24h / 100):.2f}")
                else:
                    not_included.append([prd_id, percentage_change])
                time.sleep(0.01)

    print(not_included)
main()