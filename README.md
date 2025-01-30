# Adventure Ted Hackathon 2025 – WETH/USDT Price Analysis

## Overview

This project fetches **WETH/USDT** trades from:

1. **Uniswap v2**  
2. **Uniswap v3**  
3. **Cowswap**  

…specifically for **a chosen date range** in January 2024, then compares each trade’s realized price to **Binance’s mid-price** for the same pair/time range. Finally, it produces a plot of **average cost vs. trade size** (one line per exchange).

> **Note**: For demonstration, we used a **1-day** range in January 2024 (instead of the entire month) to keep the data more manageable and avoid extremely long fetch times. You can easily modify the scripts to fetch the full month or any other custom range.

**Cowswap Endpoint Note**: We have not found a stable subgraph endpoint for Cowswap in January 2024. If a valid endpoint becomes available, you can update `fetch_cowswap.py` accordingly. Until then, our final chart shows only Uniswap v2 and Uniswap v3.

---

## Project Structure


1. **`data/`**: Contains your raw data files (Binance parquet, plus the CSV outputs from fetching Uniswap trades).  
2. **`results/`**: Holds the final `average_costs.png` plot.  
3. **`scripts/`**: Python scripts to fetch data, analyze, and generate the plot.  

---

## 1. Installation & Setup

1. **Clone** or **download** this repository (`hackathon_project`).  
2. **Install dependencies** (Python 3.10 recommended):
   ```bash
   pip install -r requirements.txt

conda create -n hackathon_py310 python=3.10
conda activate hackathon_py310
pip install -r requirements.txt

# Fetch Uniswap v2 data (1-day range)
python scripts/fetch_uniswap_v2.py

# Fetch Uniswap v3 data (1-day range)
python scripts/fetch_uniswap_v3.py

# Optionally fetch Cowswap data (requires valid endpoint)
python scripts/fetch_cowswap.py

# Analyze and merge with Binance data
python scripts/analyze.py

# Plot final results
python scripts/plot_results.py

bash scripts/run_all.sh

---

**Explanation**:  
- This README clarifies **every** step, notes the 1-day range usage, and mentions the missing Cowswap endpoint. After copying it into your `README.md`, readers should fully understand how to run your hackathon project.

