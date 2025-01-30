#!/usr/bin/env python3
"""
Fetch Uniswap v2 WETH/USDT trades for Jan 2024 from The Graph subgraph.
Outputs CSV: data/uniswap_v2_jan2024.csv
"""

import requests
import pandas as pd
import time
import os
from datetime import datetime

UNISWAP_V2_SUBGRAPH = "https://gateway.thegraph.com/api/288c326f563ea1c902796752e5b77164/subgraphs/id/EYCKATKGBKLWvSfwvBjzfCBmGwYNdVkduYXVivCsLRFu"


def fetch_uniswap_v2_trades(start_timestamp, end_timestamp, batch_size=1000, token_pair=("WETH", "USDT")):
    """
    Fetch swap events for WETH/USDT from Uniswap v2 subgraph in [start_timestamp, end_timestamp).
    Uses simple GraphQL pagination. 
    Returns a Pandas DataFrame.
    """
    
    # The Graph endpoint might limit how many results per query (1000 for example).
    # We'll use a 'last_id' approach for pagination.
    
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
            amount0In
            amount0Out
            amount1In
            amount1Out
            pair {
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
        print(f"Fetching from last_id={last_id} ...")
        response = requests.post(
        UNISWAP_V2_SUBGRAPH,
        json={"query": query, "variables": variables}
        )

        print("Response received.")
        response_json = response.json()
        
        if "data" not in response_json or "swaps" not in response_json["data"]:
            # If an error or unexpected structure occurs
            print("Error in response:", response_json)
            break
        
        swaps = response_json["data"]["swaps"]
        if not swaps:
            # No more data
            break
        
        all_swaps.extend(swaps)
        print(f"Got {len(swaps)} new swaps, total so far: {len(all_swaps)}")

        
        last_id = swaps[-1]["id"]  # update pagination key
        
        # Rate limit
        time.sleep(0.2)
    
    df = pd.DataFrame(all_swaps)

    if df.empty:
      print("Fetched 0 swaps. Exiting fetch_uniswap_v2_trades.")
      return df

    
    # Convert timestamp to int
    df["timestamp"] = df["timestamp"].astype(int)
    # Filter only WETH/USDT
    # For v2, we might need to ensure the pair tokens are exactly WETH and USDT
    def is_weth_usdt(pair_info):
        t0 = pair_info["token0"]["symbol"]
        t1 = pair_info["token1"]["symbol"]
        return (t0 in token_pair) and (t1 in token_pair)
    
    df = df[df["pair"].apply(is_weth_usdt)]
    
    return df


if __name__ == "__main__":
    # For January 2024
    start_dt = datetime(2024, 1, 1)
    end_dt   = datetime(2024, 1, 2)
    
    start_timestamp = int(start_dt.timestamp())
    end_timestamp   = int(end_dt.timestamp())
    
    df_v2 = fetch_uniswap_v2_trades(start_timestamp, end_timestamp)
    print(f"Fetched {len(df_v2)} Uniswap v2 swaps")
    
    # Ensure data folder
    os.makedirs("data", exist_ok=True)
    
    out_csv = "data/uniswap_v2_jan2024.csv"
    df_v2.to_csv(out_csv, index=False)
    print(f"Saved to {out_csv}")
