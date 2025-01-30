#!/usr/bin/env bash
# A single script to run all steps end-to-end

echo "=== 1) Fetching Uniswap v2 Data ==="
python scripts/fetch_uniswap_v2.py

echo "=== 2) Fetching Uniswap v3 Data ==="
python scripts/fetch_uniswap_v3.py

echo "=== 3) Fetching Cowswap Data ==="
python scripts/fetch_cowswap.py

echo "=== 4) Running Analysis ==="
python scripts/analyze.py

echo "=== 5) Plotting Results ==="
python scripts/plot_results.py

echo "All steps completed. See results/average_costs.png"
