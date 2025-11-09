import requests
import pandas as pd
import logging
from datetime import date
import os

def extract_data():
    logging.info("Extract: fetching CoinGecko markets...")
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {"vs_currency": "usd", "order": "market_cap_desc", "per_page": 10, "page": 1}
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    df = pd.DataFrame(data)

    # Save raw snapshot
    os.makedirs(os.path.join("data", "data_raw"), exist_ok=True)
    raw_path = os.path.join("data", "data_raw", f"crypto_raw_{date.today()}.csv")
    df.to_csv(raw_path, index=False)
    logging.info(f"Extract: saved raw snapshot to {raw_path} ({len(df)} rows)")
    return df

