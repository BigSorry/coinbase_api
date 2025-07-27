import glob
import os
import math
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.graph_objects as go
from plotly.subplots import make_subplots

def readCSV(folder_path):
    all_files = os.listdir(folder_path)
    all_dfs = {}
    for file_name in all_files:
        if file_name.endswith('.csv'):
            file_path = os.path.join(folder_path, file_name)
            df = pd.read_csv(file_path, index_col=None, header=0)
            df['date'] = pd.to_datetime(df['timestamp'], unit='s')
            df.set_index('date', inplace=True)
            # Sort and drop duplicate indices, keeping the first occurrence
            df_unique = df.sort_index().loc[~df.index.duplicated(keep='first')]
            # Filter for February 2025 and later
            all_dfs[file_name] = df_unique[df_unique.index >= '2025-07-10']

    return all_dfs

def getVWAP(df):
    # Step 1: Calculate the Typical Price for each row
    df['typical_price'] = (df['high'] + df['low'] + df['close']) / 3
    # Step 2: Calculate Price x Volume
    df['Price x Volume'] = df['typical_price'] * df['volume']
    # Step 3: Calculate Cumulative Price x Volume and Cumulative Volume
    df['Cumulative Price x Volume'] = df['Price x Volume'].cumsum()
    df['Cumulative Volume'] = df['volume'].cumsum()
    # Step 4: Calculate VWAP
    return df['Cumulative Price x Volume'] / df['Cumulative Volume']

# time for timestamp, refactor this method
def normalizeData(df, option=2):
    columns = df.columns
    for column in columns:
        if "time" not in column.lower():
            if option == 1:
                df[column] = df[column] / df[column] .iloc[0]
            elif option == 2:
                df[column] = (df[column] - df[column].min()) / (df[column].max() - df[column].min())

def plotData(df, title_text):
    # Resample to daily frequency and take the last closing price of each day
    df.set_index('date', inplace=True)
    #normalizeData(df)
    weekly_data = df['close'].resample('W').mean()
    weekly_data = weekly_data.to_frame()
    # Calculate the percentage increase over the last 24 hours
    #weekly_data["percentage_change"] = weekly_data.pct_change() * 100
    vwap_data = getVWAP(df)
    plt.plot(vwap_data.index, vwap_data, label=title_text)
    plt.plot(df.index, df["close"], label=title_text)

def plotSimple(ax, df, title_text, norm_option=1):
    # Resample to daily frequency and take the last closing price of each day
    local_df = df.copy()
    normalizeData(local_df, norm_option)
    weekly_data_close = local_df['close'].resample('D').last()
    weekly_data_close = weekly_data_close.to_frame()
    # weekly_data_open = df['open'].resample('W').first()
    # weekly_data_open = weekly_data_open.to_frame()
    # plt.plot(weekly_data_open.index, weekly_data_open, label=title_text+"Open")
    ax.plot(weekly_data_close.index, weekly_data_close, label=title_text+"Close")

def setup_plot(title_text: str, xlabel: str, ylabel: str) -> None:
    plt.title(title_text)
    plt.xlabel(xlabel, fontsize=12)
    plt.ylabel(ylabel, fontsize=12)
    #plt.yscale('log')
    plt.grid(True)
    plt.xticks(rotation=45)
    plt.legend(loc='upper left')

def plotCorr(correlation_matrix):
    # Plot correlation heatmap
    plt.figure(figsize=(16, 12))
    sns.heatmap(correlation_matrix, annot=True, cmap="coolwarm", fmt=".2f", linewidths=0.5)
    plt.title("Correlation Matrix Heatmap")
    plt.show()

def plotCandles(dfs_dict, plots_per_fig=3):
    # Assuming `all_dfs` is a dictionary where keys are coin pairs and values are DataFrames
    pair_names = list(dfs_dict.keys())
    num_pairs = len(pair_names)
    rows = math.ceil(num_pairs / plots_per_fig / 2)

    # Create a new figure with subplots
    fig, axes = plt.subplots(nrows=rows, ncols=2, figsize=(10, 5 * rows), sharex=True)
    axes = axes.flatten()

    # Plot each coin pair in its own subplot
    ax_index = 0
    for index, pair_name in enumerate(pair_names):
        ax = axes[ax_index]
        seL_df = dfs_dict[pair_name]
        plotSimple(ax, seL_df, pair_name, norm_option=2)
        if index % plots_per_fig == 0:
            ax_index += 1
            ax.legend(loc='upper left')


def filterPairs(coins_dict, coin_names):
    # Filtered dictionary
    filtered = {}
    for coin_name in coin_names:
        key_name = coin_name + ".csv"
        if key_name in coins_dict:
            filtered[key_name] = coins_dict[key_name]

    return filtered

def showCandles():
    folder_path = "data/historical_data/"
    dataframes_dict = readCSV(folder_path)
    # coins_sel = ["BTC-USD", "ADA-USD", "ETH-USD"]
    # filtered = filterPairs(dataframes_dict, coins_sel)
    plotCandles(dataframes_dict, plots_per_fig = 5)

showCandles()
plt.show()
# Show plot
