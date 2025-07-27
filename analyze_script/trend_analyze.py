import requests
import pandas as pd
import numpy as np
import datetime
from sklearn.decomposition import PCA
import matplotlib.pyplot as plt
import api_scripts.get_request as api_get
import util

def filterVolume(df, largest_n=10):
    df["usd_volume"] = df.volume * df.close
    usd_volumes = df.groupby("pair")["usd_volume"].sum()
    top_n = usd_volumes.nlargest(largest_n).index.tolist()
    # Step 2: Filter rows that contain any of these keywords
    pattern = '|'.join(top_n)
    filtered_df = df[df['pair'].str.contains(pattern, case=False, na=False)]
    return filtered_df

def get_pairs():
    ids_path = "../data/coin_pairs_vol_100m_30days.pkl"
    ids_list = util.readPickle(ids_path)
    usd_ids = []
    stable_coin = {"USDT", "USDC", "DAI", "TUSD", "EUR", "GBP", "BUSD"}
    for pair in ids_list:
        base, second = pair.split("-")
        if base not in stable_coin and second.endswith("USD"):
            usd_ids.append(pair)
    return usd_ids

def get_price_history(coin_id, days=7):
    historical_data_df = api_get.getPriceHistory(coin_id, days, granularity_unit=3600, df_return=True)
    filtered_df = filterVolume(historical_data_df)# days
    # Pivot so each pair is a column and index is timestamp
    pivot_df = filtered_df.pivot(index='timestamp', columns='pair', values='close')
    # Drop pairs with too much missing data if necessary
    pivot_df = pivot_df.dropna(thresh=int(0.9 * len(pivot_df)), axis=1)
    return pivot_df

# Step 1: Define coin list and time window
top_pairs = [
    "BTC-USD", "ETH-USD", "SOL-USD", "ADA-USD", "AVAX-USD",
    "MATIC-USD", "DOT-USD", "LINK-USD", "UNI-USD", "NEAR-USD"
]
top_pairs = get_pairs()
# Step 2: Build a combined DataFrame
df = get_price_history(top_pairs)
# Calculate daily % returns
returns = df.pct_change().dropna()

# Optional: fill any remaining NaNs
returns = returns.fillna(0)

# Run PCA to find main market direction
pca = PCA(n_components=1)
market_trend = pca.fit_transform(returns)

# Add market trend to the DataFrame
returns['market_trend'] = market_trend

# Correlation of each coin with market trend
correlations = returns.corr()['market_trend'].drop('market_trend').sort_values()

# Output:
print("Coins most aligned with the market trend:")
print(correlations[10:])

print("\nCoins deviating or lagging the trend:")
print(correlations[:10])

# Optional: Plot normalized price trends
normalized = df / df.iloc[0] * 100
normalized.plot(figsize=(12, 6), title="Normalized Price Trends (%)")
plt.ylabel("Price relative to day 1 (%)")
plt.show()