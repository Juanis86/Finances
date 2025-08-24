"""Utilities for downloading global stock data using yfinance.

This module provides helper functions to obtain price series for
international markets and save them to CSV files for later processing.
"""

from __future__ import annotations

from pathlib import Path
from typing import Union

import pandas as pd
import yfinance as yf


def get_price_series(symbol: str, start: str, end: str) -> pd.DataFrame:
    """Download OHLCV data for ``symbol`` between ``start`` and ``end``.

    Parameters
    ----------
    symbol: str
        Ticker symbol recognised by Yahoo Finance.
    start: str
        Start date in ``YYYY-MM-DD`` format.
    end: str
        End date in ``YYYY-MM-DD`` format.

    Returns
    -------
    pandas.DataFrame
        DataFrame indexed by date with columns ``close``, ``var``, ``open``,
        ``max``, ``min``, ``volume``, ``ticker``, ``country`` and ``asset``.
    """
    data = yf.download(symbol, start=start, end=end, progress=False)
    if data.empty:
        return pd.DataFrame(
            columns=["close", "var", "open", "max", "min", "volume", "ticker", "country", "asset"]
        )

    data.rename(
        columns={
            "Open": "open",
            "High": "max",
            "Low": "min",
            "Close": "close",
            "Volume": "volume",
        },
        inplace=True,
    )
    data["var"] = data["close"].pct_change().fillna(0)
    data["ticker"] = symbol
    data["country"] = "global"
    data["asset"] = "stocks"
    data.index.name = "dateTime"
    return data[["close", "var", "open", "max", "min", "volume", "ticker", "country", "asset"]]


def save_series_to_csv(df: pd.DataFrame, path: Union[str, Path]) -> None:
    """Save the provided price series ``df`` to ``path``.

    The directory for ``path`` is created if it does not already exist.
    """
    out_path = Path(path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path)
