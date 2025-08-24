"""Utility functions to download market and macro data from various providers.

This module relies on third-party APIs such as FRED, Yahoo Finance and
Alpha Vantage. The data returned by each helper is normalised so that the
columns share a common naming convention and are stored in the
corresponding directories defined in :mod:`settings`.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from pandas_datareader import data as pdr
import yfinance as yf
from alpha_vantage.timeseries import TimeSeries
from alpha_vantage.foreignexchange import ForeignExchange
import finnhub

from settings import (
    path_macro,
    path_stocks,
    path_currencies,
    FRED_API_KEY,
    ALPHA_VANTAGE_API_KEY,
)


def _ensure_path(path: Path) -> Path:
    """Create *path* if it does not exist and return it."""
    path.mkdir(parents=True, exist_ok=True)
    return path


def fetch_fred(
    series_id: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    path: Path = path_macro,
) -> pd.DataFrame:
    """Download a series from FRED and store it as ``<series_id>.csv``.

    Parameters
    ----------
    series_id:
        Identifier of the FRED series, e.g. ``"GDP"``.
    start, end:
        Optional date strings (``YYYY-MM-DD``) to bound the request.
    path:
        Destination directory. Defaults to :data:`settings.path_macro`.
    """
    df = pdr.DataReader(series_id, "fred", start=start, end=end, api_key=FRED_API_KEY)
    df = df.rename(columns={series_id: "value"}).sort_index()
    df.index.name = "date"
    file_path = _ensure_path(path) / f"{series_id}.csv"
    df.to_csv(file_path)
    return df


def fetch_yahoo(
    symbol: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    path: Path = path_stocks,
) -> pd.DataFrame:
    """Download daily OHLC data from Yahoo Finance for *symbol*.

    The resulting dataframe contains the columns ``open``, ``high``, ``low``,
    ``close``, ``adj_close`` and ``volume``.
    """
    df = yf.download(symbol, start=start, end=end, progress=False)
    df = df.rename(
        columns={
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Adj Close": "adj_close",
            "Volume": "volume",
        }
    )
    df.index.name = "date"
    file_path = _ensure_path(path) / f"{symbol}.csv"
    df.to_csv(file_path)
    return df


def fetch_ecb_fx(
    base: str,
    symbols: list[str],
    path: Path = path_currencies,
) -> pd.DataFrame:
    """Download latest FX rates from the ECB.

    Rates are retrieved via the https://api.exchangerate.host/ endpoint. The
    returned dataframe contains the columns ``date``, ``currency`` and
    ``rate``. A CSV file named ``<base>.csv`` is stored in
    :data:`settings.path_currencies`.

    Parameters
    ----------
    base:
        Base currency for the conversion, e.g. ``"EUR"``.
    symbols:
        List of target currency codes.
    path:
        Destination directory. Defaults to :data:`settings.path_currencies`.
    """
    url = "https://api.exchangerate.host/latest"
    params = {"base": base, "symbols": ",".join(symbols)}
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    date = data.get("date")
    rates = data.get("rates", {})
    df = pd.DataFrame(
        [{"date": date, "currency": cur, "rate": rate} for cur, rate in rates.items()]
    )

    file_path = _ensure_path(path) / f"{base}.csv"
    df.to_csv(file_path, index=False)
    return df


def fetch_alpha_vantage_fx(
    from_currency: str,
    to_currency: str,
    path: Path = path_currencies,
) -> pd.DataFrame:
    """Download daily FX rates from Alpha Vantage.

    The returned dataframe includes the columns ``open``, ``high``, ``low``
    and ``close``.
    """
    fx = ForeignExchange(key=ALPHA_VANTAGE_API_KEY, output_format="pandas")
    data, _ = fx.get_currency_exchange_daily(from_currency, to_currency)
    df = data.rename(
        columns={
            "1. open": "open",
            "2. high": "high",
            "3. low": "low",
            "4. close": "close",
        }
    )
    df.index = pd.to_datetime(df.index)
    df.index.name = "date"
    file_path = _ensure_path(path) / f"{from_currency}_{to_currency}.csv"
    df.to_csv(file_path)
    return df


def fetch_alpha_vantage_stock(
    symbol: str,
    path: Path = path_stocks,
) -> pd.DataFrame:
    """Download daily OHLC data from Alpha Vantage for *symbol*.

    The dataframe contains ``open``, ``high``, ``low``, ``close``,
    ``adj_close`` and ``volume``.
    """
    ts = TimeSeries(key=ALPHA_VANTAGE_API_KEY, output_format="pandas")
    data, _ = ts.get_daily_adjusted(symbol=symbol)
    df = data.rename(
        columns={
            "1. open": "open",
            "2. high": "high",
            "3. low": "low",
            "4. close": "close",
            "5. adjusted close": "adj_close",
            "6. volume": "volume",
        }
    )
    df = df[["open", "high", "low", "close", "adj_close", "volume"]]
    df.index = pd.to_datetime(df.index)
    df.index.name = "date"
    file_path = _ensure_path(path) / f"{symbol}.csv"
    df.to_csv(file_path)
    return df


def fetch_finnhub_stock(
    symbol: str,
    start: str,
    end: str,
    api_key: str,
) -> pd.DataFrame:
    """Download daily OHLC data from Finnhub for *symbol*.

    The resulting dataframe contains the columns ``open``, ``high``, ``low``,
    ``close`` and ``volume``.
    """
    client = finnhub.Client(api_key=api_key)
    start_ts = int(pd.Timestamp(start).timestamp())
    end_ts = int(pd.Timestamp(end).timestamp())
    data = client.stock_candles(symbol, "D", start_ts, end_ts)
    if data.get("s") != "ok":
        raise ValueError(f"Finnhub returned status {data.get('s')}")
    df = pd.DataFrame(
        {
            "open": data["o"],
            "high": data["h"],
            "low": data["l"],
            "close": data["c"],
            "volume": data["v"],
        },
        index=pd.to_datetime(data["t"], unit="s"),
    )
    df.index.name = "date"
    file_path = _ensure_path(path_stocks) / f"{symbol}.csv"
    df.to_csv(file_path)
    return df


__all__ = [
    "fetch_fred",
    "fetch_yahoo",
    "fetch_ecb_fx",
    "fetch_alpha_vantage_fx",
    "fetch_alpha_vantage_stock",
    "fetch_finnhub_stock",
]
