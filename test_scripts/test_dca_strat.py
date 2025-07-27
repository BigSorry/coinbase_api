import pandas as pd
import numpy as np
# Load your historical BTC daily price data (date, close price)
# For example, download daily BTC prices CSV from https://www.coingecko.com or other sources
# The CSV should have columns: 'date' and 'close'
MONTHLY_BUDGET = 150
NUM_BUYS = 1 # buys per month
BUY_WINDOW_DAYS = 30
TARGET_DIP_PERCENT = 0.03

def filter_year(btc_data, year):
    btc_data['date'] = pd.to_datetime(btc_data['timestamp'], unit='s')
    filtered_df = btc_data[btc_data['date'].dt.year == year]
    filtered_df.set_index('date', inplace=True)
    # Sort and drop duplicate indices, keeping the first occurrence
    df_unique = filtered_df.sort_index().loc[~filtered_df.index.duplicated(keep='first')]

    return df_unique

# 1) Fixed DCA simulation: buy equal amounts on fixed schedule, at closing price
def fixed_dca(btc_prices):
    total_btc = 0
    total_usd = 0
    interval = 30*24 # 30 days next buy times 24 hours
    buy_amount_usd = MONTHLY_BUDGET
    prices_history = []
    for i in range(0, len(btc_prices), interval):
        price = btc_prices.iloc[i]
        btc_bought = buy_amount_usd / price
        total_btc += btc_bought
        total_usd += buy_amount_usd
        prices_history.append(price)

    avg_price = total_usd / total_btc

    return total_btc, avg_price, prices_history


# 2) Flexible DCA simulation
def flexible_dca(btc_prices):
    total_btc = 0
    total_usd = 0
    buy_amount_usd = MONTHLY_BUDGET / NUM_BUYS
    last_buy_price = btc_prices.iloc[0]  # start reference price

    # Iterate in windows of BUY_WINDOW_DAYS days spaced evenly for NUM_BUYS buys per month
    rows_per_month = 24*30 # 24 hours csv granularity
    days_per_buy = rows_per_month // NUM_BUYS
    buy_indices = [i for i in range(0, len(btc_prices), days_per_buy)]
    prices_history = []
    for idx in buy_indices:
        window_start = idx
        window_end = min(idx + BUY_WINDOW_DAYS*24, len(btc_prices) - 1)
        window_prices = btc_prices.iloc[window_start:window_end + 1]

        # Find if price dips below target
        target_price = last_buy_price * (1 - TARGET_DIP_PERCENT)
        dips = window_prices[window_prices <= target_price]

        if not dips.empty:
            # Buy at lowest dip price
            buy_price = dips.min()

        else:
            # No dip, buy at last day price in window
            buy_price = window_prices.iloc[-1]

        prices_history.append(buy_price)

        btc_bought = buy_amount_usd / buy_price
        total_btc += btc_bought
        total_usd += buy_amount_usd
        last_buy_price = buy_price

    avg_price = total_usd / total_btc
    return total_btc, avg_price, prices_history

def averageMonth(variable_price_list):
    arr = np.array(variable_price_list)
    chunk_size = NUM_BUYS
    step = NUM_BUYS
    averages = [arr[i:i + chunk_size].mean() for i in range(0, len(arr), step) if
                len(arr[i:i + chunk_size]) == chunk_size]

    return averages

if __name__ == "__main__":
    btc_data = pd.read_csv('../data/BTC-USD.csv')
    years = [i for i in range(2021, 2026)]

    for year in years:
        print("Year ", year)
        filtered_df = filter_year(btc_data, year)
        prices = filtered_df['close']

        fixed_btc, fixed_avg_price, prices_history = fixed_dca(prices)
        flexible_btc, flexible_avg_price, prices_history_flex = flexible_dca(prices)
        # Reshape into (-1, 4) => rows of 4 elements each
        prices_history_flex_month_avg = averageMonth(prices_history_flex)

        # for i in range(len(prices_history)):
        #     fixed_prc = prices_history[i]
        #     fixed_prc_flex = prices_history_flex_month_avg[i]
        #     print(fixed_prc, fixed_prc_flex)
        total = fixed_btc* fixed_avg_price
        total_flex = flexible_btc* flexible_avg_price
        print(f"Fixed DCA: Total BTC = {fixed_btc:.6f}, Average Buy Price = ${fixed_avg_price:.2f}, {total:.2f}")
        print(f"Flexible DCA: Total BTC = {flexible_btc:.6f}, Average Buy Price = ${flexible_avg_price:.2f}, {total_flex:.2f}")
        print()