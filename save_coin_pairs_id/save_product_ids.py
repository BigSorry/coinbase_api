import products.get_products as get_prd
import products.get_request as get_prd_api
import util
import datetime
import numpy as np

def savePickleIds():
    filter_currency = "usd"
    all_ids = get_prd.getProductIds()
    base_url = 'https://api.exchange.coinbase.com/products/{}/{}'
    filtered_ids = get_prd.getProductIds()
    util.savePickle("./usdc_ids.pkl", filtered_ids)

def filterVolume(all_pairs_stats):
    sel_pairs = []
    volume_filter = 50*1e6
    volume_filter = 100*1e6
    for pair_name, stat_arr in all_pairs_stats.items():
        # Quantity in Euros or USDC
        stats_np = np.array(stat_arr)
        lhoc_prices_avg = stats_np[:, 1:4].mean(axis=1)
        volumes = stats_np[:, 5]
        month_vol = np.sum(lhoc_prices_avg * volumes)
        if month_vol > volume_filter:
            sel_pairs.append(pair_name)

    return sel_pairs

def saveIds():
    all_pairs = get_prd.getProductIds()
    coin_filter_keywords = ["EUR", "USD"]
    # Pairs must contain either EUR or USDC
    filter_pair_name = [pair for pair in all_pairs if any(keyword in pair for keyword in coin_filter_keywords)]
    granularity = 86400
    timestamp_start = datetime.datetime.now() - datetime.timedelta(days=30)
    timestamp_end = datetime.datetime.now()

    base_url = 'https://api.exchange.coinbase.com/products/{}/{}'
    url_param = f"candles?granularity={granularity}&start={timestamp_start}&end={timestamp_end}"
    filtered_pair_stats = get_prd_api.getAPIData(base_url, filter_pair_name, url_param)
    selected_ids = filterVolume(filtered_pair_stats)

    return selected_ids

path = "../data/coin_pairs_vol_100m_30days.pkl"
selected_ids = saveIds()
util.savePickle(path, selected_ids)
ids = util.readPickle(path)
print(len(ids))
for id in ids:
    print(id)