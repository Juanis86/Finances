import requests
import pandas as pd
from pathlib import Path
from settings import path_crypto, COINMARKETCAP_API_KEY

API_URL = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"

def fetch_cmc_markets(limit: int, convert: str, path: Path = path_crypto):
    """Fetch market data for the top cryptocurrencies from CoinMarketCap.

    Parameters
    ----------
    limit : int
        Number of cryptocurrencies to retrieve.
    convert : str
        The fiat or cryptocurrency symbol to convert market data to.
    path : Path, optional
        Directory where the data files will be stored.

    For each asset returned by the API, a CSV file will be created or updated
    inside ``path`` using the asset ``slug`` as filename. Subsequent executions
    append new rows to existing files, providing a history of market snapshots.
    """
    headers = {"X-CMC_PRO_API_KEY": COINMARKETCAP_API_KEY}
    params = {"limit": limit, "convert": convert}
    response = requests.get(API_URL, headers=headers, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()["data"]

    for asset in data:
        df_asset = pd.json_normalize(asset, sep="_")
        # Convert timestamp columns to datetime
        for col in [c for c in df_asset.columns if c.endswith("last_updated")]:
            df_asset[col] = pd.to_datetime(df_asset[col])
        file_path = path / f"{asset['slug']}.csv"
        if file_path.exists():
            existing = pd.read_csv(file_path)
            df_all = pd.concat([existing, df_asset], ignore_index=True)
        else:
            df_all = df_asset
        df_all.to_csv(file_path, index=False)

    return data
