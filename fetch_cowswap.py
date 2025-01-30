#!/usr/bin/env python3
"""
Fetch Cowswap WETH/USDT trades for Jan 2024. 
Cowswap also has a The Graph subgraph, but the endpoint can differ.
Below is an example approach.
Outputs CSV: data/cowswap_jan2024.csv
"""

import requests
import pandas as pd
import time
import os
from datetime import datetime

# Note: The following subgraph is an example from CoW Protocol docs:
COWSWAP_SUBGRAPH = "https://gateway.thegraph.com/api/df43a2bc1070b588a29f977563828492/subgraphs/id/H2gFH3qBTB1GPzy1xTbf85P9JMhq6sHGMmu1JKUmA6bg"

def fetch_cowswap_trades(start_timestamp, end_timestamp, batch_size=1000, token_pair=("WETH", "USDT")):
    """
    Fetch CowSwap (CoW Protocol) trades for WETH/USDT in [start_timestamp, end_timestamp).
    Returns a Pandas DataFrame.
    """
    all_orders = []
    last_id = ""
    
    while True:
        query = """
        query($startTime: Int!, $endTime: Int!, $lastID: String!) {
          orders(
            where: {
              creationTimestamp_gte: $startTime,
              creationTimestamp_lt:  $endTime,
              id_gt: $lastID
            }
            orderBy: id
            orderDirection: asc
            first: %d
          ) {
            id
            creationTimestamp
            sellToken { symbol }
            buyToken { symbol }
            sellAmount
            buyAmount
          }
        }
        """ % batch_size
        
        variables = {
            "startTime": int(start_timestamp),
            "endTime": int(end_timestamp),
            "lastID": last_id
        }
        
        response = requests.post(COWSWAP_SUBGRAPH, json={"query": query, "variables": variables})
        response_json = response.json()
        
        if "data" not in response_json or "orders" not in response_json["data"]:
            print("Error in response:", response_json)
            break
        
        orders = response_json["data"]["orders"]
        if not orders:
            break
        
        all_orders.extend(orders)
        
        last_id = orders[-1]["id"]
        time.sleep(0.2)
    
    df = pd.DataFrame(all_orders)
    df["creationTimestamp"] = df["creationTimestamp"].astype(int)
    
    # Filter only WETH/USDT
    def is_weth_usdt(sell_token, buy_token):
        tokens = {sell_token, buy_token}
        return tokens == set(token_pair)
    
    mask = df.apply(lambda row: is_weth_usdt(row["sellToken"]["symbol"], row["buyToken"]["symbol"]), axis=1)
    df = df[mask]
    
    return df


if __name__ == "__main__":
    # For January 2024
    start_dt = datetime(2024, 1, 1)
    end_dt   = datetime(2024, 1, 2)
    
    start_timestamp = int(start_dt.timestamp())
    end_timestamp   = int(end_dt.timestamp())
    
    df_cw = fetch_cowswap_trades(start_timestamp, end_timestamp)
    print(f"Fetched {len(df_cw)} Cowswap orders")
    
    os.makedirs("data", exist_ok=True)
    
    out_csv = "data/cowswap_jan2024.csv"
    df_cw.to_csv(out_csv, index=False)
    print(f"Saved to {out_csv}")
