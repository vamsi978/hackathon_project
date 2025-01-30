#!/usr/bin/env python3
"""
Analyze the DEX trades vs. Binance mid-price, compute average cost difference by trade size.
Outputs intermediate aggregated CSV files for each DEX.
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime

def load_binance_trades(parquet_or_csv):
    """
    Load Binance data from a Parquet or CSV file.
    Expects columns: ['timestamp','datetime','mid_price','volume'] (based on your check.py output).
    """
    if parquet_or_csv.endswith(".parquet"):
        df = pd.read_parquet(parquet_or_csv)
    else:
        df = pd.read_csv(parquet_or_csv)
    
    # If you prefer to rely on 'timestamp', do:
    # df["timestamp"] = pd.to_datetime(df["timestamp"], unit='ms')
    # Otherwise, your file already has 'datetime' as a string—convert to a real datetime dtype.
    df["datetime"] = pd.to_datetime(df["datetime"])
    
    return df

def compute_mid_prices(binance_df, freq="1min"):
    """
    Approximate mid-price by averaging 'mid_price' in each time bucket (1min).
    The file has columns ['datetime','mid_price'], so we use 'datetime' as our time index.
    """
    binance_df = binance_df.sort_values("datetime").copy()
    binance_df.set_index("datetime", inplace=True)

    # Resample on 1-min intervals
    df_resampled = binance_df["mid_price"].resample(freq).mean().ffill()

    # Convert to DataFrame named 'mid_price'
    df_resampled = df_resampled.to_frame(name="mid_price").reset_index()
    # So now we have columns: ['datetime','mid_price']
    return df_resampled

def compute_realized_prices_uniswap_v2(df):
    """
    For Uniswap v2, we have columns: [amount0In, amount0Out, amount1In, amount1Out, timestamp].
    We treat 'amount0Out' as WETH sold, 'amount1In' as USDT received.
    Realized price = USDT received / WETH sold.
    """
    numeric_cols = ["amount0In", "amount0Out", "amount1In", "amount1Out"]
    for col in numeric_cols:
        df[col] = df[col].astype(float)
    
    df["realized_price"] = np.where(
        df["amount0Out"] > 0,
        df["amount1In"] / df["amount0Out"],   # WETH -> USDT
        df["amount0In"] / df["amount1Out"]    # USDT -> WETH
    )
    # Convert timestamp to datetime
    df["timestamp_dt"] = pd.to_datetime(df["timestamp"], unit='s')
    return df

def compute_realized_prices_uniswap_v3(df):
    """
    For Uniswap v3, we have columns: [amount0, amount1, timestamp].
    We do a naive approach for realized price = amount1/amount0, etc.
    """
    df["amount0"] = df["amount0"].astype(float)
    df["amount1"] = df["amount1"].astype(float)
    
    df["realized_price"] = np.where(
        df["amount0"] > 0,
        df["amount1"] / df["amount0"],
        df["amount0"] / df["amount1"]
    )
    df["timestamp_dt"] = pd.to_datetime(df["timestamp"], unit='s')
    return df

def compute_realized_prices_cowswap(df):
    """
    For Cowswap, columns: [sellAmount, buyAmount, creationTimestamp].
    Realized price = buyAmount / sellAmount.
    """
    df["sellAmount"] = df["sellAmount"].astype(float)
    df["buyAmount"]  = df["buyAmount"].astype(float)
    df["realized_price"] = df["buyAmount"] / df["sellAmount"]
    
    df["timestamp_dt"] = pd.to_datetime(df["creationTimestamp"], unit='s')
    return df

def merge_with_midprice(df, midprices_df):
    """
    Merge DEX trades with the nearest Binance mid-price via pandas.merge_asof.
    DEX data has 'timestamp_dt'; midprices_df has 'datetime' from compute_mid_prices().
    """
    df_sorted = df.sort_values("timestamp_dt").reset_index(drop=True)

    # rename 'datetime' to a uniform name, or directly use 'datetime' below
    mid_sorted = midprices_df.sort_values("datetime").reset_index(drop=True)

    merged = pd.merge_asof(
        df_sorted,
        mid_sorted,
        left_on="timestamp_dt",
        right_on="datetime",
        direction="nearest"
    )
    merged["price_diff"] = merged["realized_price"] - merged["mid_price"]
    return merged

def bucket_by_trade_size(df, size_col, n_buckets=10):
    """
    Group trades by trade size in USD (e.g., 'amount1In' or 'buyAmount' as USDT).
    """
    df["bucket"] = pd.qcut(df[size_col], q=n_buckets, duplicates='drop')
    grouped = df.groupby("bucket")["price_diff"].mean().reset_index()
    grouped.rename(columns={"price_diff": "avg_cost_diff"}, inplace=True)
    return grouped

def main_analysis():
    # 1) Load Binance data
    binance_file = "data/cex_trades_binance_ETH_USDT-2024-01.parquet"
    binance_trades = load_binance_trades(binance_file)
    midprices = compute_mid_prices(binance_trades)  # columns: ['datetime','mid_price']

    # 2) Uniswap v2
    df_v2 = pd.read_csv("data/uniswap_v2_jan2024.csv")
    df_v2 = compute_realized_prices_uniswap_v2(df_v2)
    df_v2["trade_size_usd"] = np.where(
        df_v2["amount0Out"] > 0, df_v2["amount1In"], df_v2["amount1Out"]
    ).astype(float)
    merged_v2 = merge_with_midprice(df_v2, midprices)
    v2_buckets = bucket_by_trade_size(merged_v2, "trade_size_usd", 10)
    v2_buckets.to_csv("data/uniswap_v2_aggregated.csv", index=False)

    # 3) Uniswap v3
    df_v3 = pd.read_csv("data/uniswap_v3_jan2024.csv")
    df_v3 = compute_realized_prices_uniswap_v3(df_v3)
    df_v3["trade_size_usd"] = df_v3["amount1"].abs()  # naive approach
    merged_v3 = merge_with_midprice(df_v3, midprices)
    v3_buckets = bucket_by_trade_size(merged_v3, "trade_size_usd", 10)
    v3_buckets.to_csv("data/uniswap_v3_aggregated.csv", index=False)

    # 4) Cowswap (optional; if you have no data, comment out)
    # df_cw = pd.read_csv("data/cowswap_jan2024.csv")
    # df_cw = compute_realized_prices_cowswap(df_cw)
    # df_cw["trade_size_usd"] = df_cw["buyAmount"]
    # merged_cw = merge_with_midprice(df_cw, midprices)
    # cw_buckets = bucket_by_trade_size(merged_cw, "trade_size_usd", 10)
    # cw_buckets.to_csv("data/cowswap_aggregated.csv", index=False)

    print("Analysis done. Aggregated CSVs saved in data/ folder.")

if __name__ == "__main__":
    main_analysis()

