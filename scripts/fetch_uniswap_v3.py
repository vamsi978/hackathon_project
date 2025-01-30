#!/usr/bin/env python3
"""
Fetch Uniswap v3 WETH/USDT trades for Jan 2024 from The Graph subgraph.
Outputs CSV: data/uniswap_v3_jan2024.csv
"""

import requests
import pandas as pd
import time
import os
from datetime import datetime

UNISWAP_V3_SUBGRAPH = "https://gateway.thegraph.com/api/d9b773b884c7026f7e40ca5a33b91ce9/subgraphs/id/HUZDsRpEVP2AvzDCyzDHtdc64dyDxx8FQjzsmqSg4H3B"

def fetch_uniswap_v3_trades(start_timestamp, end_timestamp, batch_size=1000, token_pair=("WETH", "USDT")):
    """
    Fetch swap events for WETH/USDT from Uniswap v3 subgraph in [start_timestamp, end_timestamp).
    Returns a Pandas DataFrame.
    """
    
    all_swaps = []
    last_id = ""
    
    while True:
        query = """
        query($startTime: Int!, $endTime: Int!, $lastID: String!) {
          swaps(
            where: {
              timestamp_gte: $startTime,
              timestamp_lt:  $endTime,
              id_gt: $lastID
            }
            orderBy: id
            orderDirection: asc
            first: %d
          ) {
            id
            timestamp
            amount0
            amount1
            pool {
              token0 { symbol }
              token1 { symbol }
            }
          }
        }
        """ % batch_size
        
        variables = {
            "startTime": int(start_timestamp),
            "endTime": int(end_timestamp),
            "lastID": last_id
        }
        
        response = requests.post(UNISWAP_V3_SUBGRAPH, json={"query": query, "variables": variables})
        response_json = response.json()
        
        if "data" not in response_json or "swaps" not in response_json["data"]:
            print("Error in response:", response_json)
            break
        
        swaps = response_json["data"]["swaps"]
        if not swaps:
            break
        
        all_swaps.extend(swaps)
        
        last_id = swaps[-1]["id"]
        
        # Rate limit
        time.sleep(0.2)
    
    df = pd.DataFrame(all_swaps)
    df["timestamp"] = df["timestamp"].astype(int)
    
    # Filter to WETH/USDT
    def is_weth_usdt(pool_info):
        t0 = pool_info["token0"]["symbol"]
        t1 = pool_info["token1"]["symbol"]
        return (t0 in token_pair) and (t1 in token_pair)
    
    df = df[df["pool"].apply(is_weth_usdt)]
    
    return df


if __name__ == "__main__":
    # For January 2024
    start_dt = datetime(2024, 1, 1)
    end_dt   = datetime(2024, 1, 2)
    
    start_timestamp = int(start_dt.timestamp())
    end_timestamp   = int(end_dt.timestamp())
    
    df_v3 = fetch_uniswap_v3_trades(start_timestamp, end_timestamp)
    print(f"Fetched {len(df_v3)} Uniswap v3 swaps")
    
    os.makedirs("data", exist_ok=True)
    
    out_csv = "data/uniswap_v3_jan2024.csv"
    df_v3.to_csv(out_csv, index=False)
    print(f"Saved to {out_csv}")
