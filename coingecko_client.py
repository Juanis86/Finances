import requests
import pandas as pd
from pathlib import Path
from settings import path_crypto

API_URL = "https://api.coingecko.com/api/v3/coins/markets"

def fetch_coins_markets(vs_currency: str, per_page: int, path: Path = path_crypto):
    """Fetch market data for the top cryptocurrencies from CoinGecko.

    Parameters
    ----------
    vs_currency : str
        The target currency of market data (usd, eur, etc.).
    per_page : int
        Total number of results per page (max 250).
    path : Path
        Directory where the data files will be stored.

    For each coin returned by the API, a CSV file will be created or
    updated inside ``path`` using the coin ``id`` as filename. Subsequent
    executions append new rows to existing files, providing a history of
    market snapshots similar to ``get_db_crypto``.
    """
    params = {
        "vs_currency": vs_currency,
        "order": "market_cap_desc",
        "per_page": per_page,
        "page": 1,
        "sparkline": "false",
    }
    response = requests.get(API_URL, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    for coin in data:
        df_coin = pd.DataFrame([coin])
        # Ensure timestamp is in datetime format
        if "last_updated" in df_coin.columns:
            df_coin["last_updated"] = pd.to_datetime(df_coin["last_updated"])
        file_path = path / f"{coin['id']}.csv"
        if file_path.exists():
            existing = pd.read_csv(file_path)
            df_all = pd.concat([existing, df_coin], ignore_index=True)
        else:
            df_all = df_coin
        df_all.to_csv(file_path, index=False)

    return data
