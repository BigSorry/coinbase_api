import time
import datetime
import util
import api_scripts.get_request as get_prd

def getIds():
    ids_path = "data/ids.pkl"
    ids_list = util.readPickle(ids_path)
    return ids_list

def getTickInfo(ids_list):
    base_url = 'https://api.exchange.coinbase.com/products/{}/{}'
    ticker_info = get_prd.getAPIData(base_url, ids_list, "ticker")
    return ticker_info

def saveBatchData(batch_data, filename):
    if batch_data:
        fieldnames = ['product_id', 'ask', 'bid', 'volume', 'trade_id',
                      'price', 'size', 'time', 'rfq_volume']
        util.saveCSV(batch_data, fieldnames, filename)
        print(f"Saved {len(batch_data)} entries to {filename}.")
    # Reset for the next batch
    batch_data = []

def main():
    ids_list = getIds()
    total_dur = 20
    saving_dur = total_dur // 2
    end_delta = datetime.timedelta(seconds=total_dur)
    save_delta = datetime.timedelta(seconds=saving_dur)
    t_save = datetime.datetime.now() + save_delta
    t_end = datetime.datetime.now() + end_delta
    prd_ticker_data = {}
    batch_data = []
    today_date = datetime.datetime.now().strftime('%Y-%m-%d')
    filename = f"./csv_data/{today_date}_ticker_data.csv"

    while t_end > datetime.datetime.now():
        ticker_info = getTickInfo(ids_list)
        for prd_id in ticker_info.keys():
            ticker_data = ticker_info[prd_id]
            trade_id = ticker_info[prd_id]['trade_id']
            if prd_id not in prd_ticker_data:
                prd_ticker_data[prd_id] = {}
            # If trade_id is unique, add the data to the batch
            if trade_id not in prd_ticker_data[prd_id]:
                prd_ticker_data[prd_id][trade_id] = True
                ticker_data["product_id"] = prd_id
                batch_data.append(ticker_data)

        if t_save <= datetime.datetime.now():
            saveBatchData(batch_data, filename)
            t_save = datetime.datetime.now() + save_delta

main()