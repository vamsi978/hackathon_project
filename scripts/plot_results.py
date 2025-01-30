#!/usr/bin/env python3
"""
Plot the average cost difference (DEX - Binance mid) vs. trade size for each DEX.
Produces results/average_costs.png
"""

import pandas as pd
import matplotlib.pyplot as plt
import os

def plot_average_costs(v2_csv="data/uniswap_v2_aggregated.csv",
                       v3_csv="data/uniswap_v3_aggregated.csv",
                       # cw_csv="data/cowswap_aggregated.csv",
                       out_file="results/average_costs.png"):
    # Load aggregated data
    df_v2 = pd.read_csv(v2_csv)
    df_v3 = pd.read_csv(v3_csv)
    # df_cw = pd.read_csv(cw_csv)
    
    # Convert the 'bucket' category to string for plotting
    df_v2["bucket_str"] = df_v2["bucket"].astype(str)
    df_v3["bucket_str"] = df_v3["bucket"].astype(str)
    # df_cw["bucket_str"] = df_cw["bucket"].astype(str)
    
    fig, ax = plt.subplots(figsize=(10, 6))
    
    ax.plot(df_v2["bucket_str"], df_v2["avg_cost_diff"], label="Uniswap v2", marker='o')
    ax.plot(df_v3["bucket_str"], df_v3["avg_cost_diff"], label="Uniswap v3", marker='o')
    # ax.plot(df_cw["bucket_str"], df_cw["avg_cost_diff"], label="Cowswap",   marker='o')
    
    ax.set_xlabel("Trade Size Buckets (USD)")
    ax.set_ylabel("Average Cost Difference (DEX Price - Binance Mid)")
    ax.set_title("Average Trading Cost vs. Trade Size (Jan 2024, WETH/USDT)")
    ax.legend()
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    os.makedirs("results", exist_ok=True)
    plt.savefig(out_file)
    print(f"Plot saved to {out_file}")

if __name__ == "__main__":
    plot_average_costs()
