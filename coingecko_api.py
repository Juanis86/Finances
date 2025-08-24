import requests
import pandas as pd
from datetime import datetime

BASE_URL = "https://api.coingecko.com/api/v3"


def get_coin_list():
    """Return a DataFrame with the list of supported coins."""
    url = f"{BASE_URL}/coins/list"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    data = response.json()
    return pd.DataFrame(data)


def get_historical_data(symbol: str, since: str) -> pd.DataFrame:
    """Return historical market data for a given ``symbol``.

    Parameters
    ----------
    symbol:
        Coin identifier as used by CoinGecko (e.g. ``bitcoin``).
    since:
        Start date in ISO format (``YYYY-MM-DD`` or with time). The
        returned DataFrame will be filtered so that the index is greater
        or equal to this date.

    Returns
    -------
    pandas.DataFrame
        Data formatted similarly to the CSV files produced for Binance.
    """
    # Retrieve the complete market chart and filter afterwards to avoid
    # multiple API calls for different ranges.
    url = f"{BASE_URL}/coins/{symbol}/market_chart"
    params = {"vs_currency": "usd", "days": "max"}
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    prices = pd.DataFrame(data.get("prices", []), columns=["dateTime", "close"])
    prices["dateTime"] = pd.to_datetime(prices["dateTime"], unit="ms")

    volumes = pd.DataFrame(data.get("total_volumes", []), columns=["dateTime", "volume"])
    volumes["dateTime"] = pd.to_datetime(volumes["dateTime"], unit="ms")

    df = pd.merge(prices, volumes, on="dateTime", how="left")

    # Replicate Binance CSV structure
    df["open"] = df["close"]
    df["max"] = df["close"]
    df["min"] = df["close"]
    df["closeTime"] = pd.NaT
    df["quoteAssetVolume"] = pd.NA
    df["numberOfTrades"] = pd.NA
    df["takerBuyBaseVol"] = pd.NA
    df["takerBuyQuoteVol"] = pd.NA
    df["ignore"] = pd.NA
    df["token"] = symbol.upper()
    df["asset"] = "cryptocurrency"

    df.set_index("dateTime", inplace=True)
    if since:
        since_dt = pd.to_datetime(since)
        df = df.loc[df.index >= since_dt]

    return df[[
        "open",
        "max",
        "min",
        "close",
        "volume",
        "closeTime",
        "quoteAssetVolume",
        "numberOfTrades",
        "takerBuyBaseVol",
        "takerBuyQuoteVol",
        "ignore",
        "token",
        "asset",
    ]]
