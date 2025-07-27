import datetime
import util
import api_scripts.get_request as api_get
import pandas as pd


def getIds():
    ids_path = "data/ids.pkl"
    ids_path = "data/coin_pairs_vol_100m_30days.pkl"
    ids_list = util.readPickle(ids_path)
    return ids_list


def getAPIData(ids_list, granularity_seconds, timestamp_start, timestamp_end):
    # First {} is product id
    # Second is the url param string
    base_url = "https://api.exchange.coinbase.com/products/{}/{}"
    url_param = f"candles?granularity={granularity_seconds}&start={timestamp_start}&end={timestamp_end}"
    ticker_info = api_get.getAPIData(base_url, ids_list, url_param)
    return ticker_info


def getLatestTS(file_name):
    df = util.readCSV(file_name)
    if df.empty:
        # start_date = end_date - pd.DateOffset(years=5)
        return datetime.datetime(2021, 1, 1, 0, 0)  # Jan 1, 2017, 00:00
    df["date"] = pd.to_datetime(df["timestamp"], unit="s")
    latest_time = df["date"].max()

    return latest_time


# TODO check if empty there is no more data coming
def getHistoricalData(prd_id, date_slices, granularity):
    coin_pair_hist_data = {}
    for i in range(len(date_slices) - 1):
        timestamp_start = date_slices[i]
        timestamp_end = date_slices[i + 1]
        historical_data = getAPIData(
            prd_id, granularity, timestamp_start, timestamp_end
        )
        if historical_data:
            coin_pair_hist_data[timestamp_start] = historical_data

    return coin_pair_hist_data


def main():
    ids_list = getIds()
    #ids_list = ["BTC-USD"]
    granularity_hours = [3600, "300h"]  # 1 hour max 300 candles
    granularity_days = [86400, "300D"]  # 1 day
    fieldnames = ["timestamp", "low", "high", "open", "close", "volume"]
    for prd_id in ids_list:
        file_name = f"data/historical_data/{prd_id}.csv"
        start_date = getLatestTS(file_name)
        end_date = datetime.datetime.now()
        # Create 300 equally spaced dates (buckets)
        date_slices = pd.date_range(
            start=start_date, end=end_date, freq=granularity_hours[1]
        )
        # Add end_date if it's not already included
        if date_slices[-1] != end_date:
            date_slices = date_slices.append(pd.DatetimeIndex([end_date]))
        prd_historical_dict = getHistoricalData(
            prd_id, date_slices, granularity_hours[0]
        )
        util.saveHistoricalDataCSV(prd_historical_dict, fieldnames, filename=file_name)


main()
