"""Utilities for retrieving fundamental financial statements from Financial Modeling Prep."""

from __future__ import annotations

import os

import pandas as pd
import requests

from settings import path_stocks


def fetch_fmp_statements(symbol: str, statement: str, period: str) -> pd.DataFrame:
    """Fetch a financial statement from Financial Modeling Prep.

    Parameters
    ----------
    symbol : str
        Stock ticker symbol recognised by Financial Modeling Prep.
    statement : str
        Type of statement to retrieve, e.g. ``"income-statement"``,
        ``"balance-sheet-statement"`` or ``"cash-flow-statement"``.
    period : str
        Reporting period to request, such as ``"annual"`` or ``"quarter"``.

    Returns
    -------
    pandas.DataFrame
        Data returned by the API. The same data is saved as CSV within
        ``path_stocks / "fundamentals"``.
    """

    base_url = f"https://financialmodelingprep.com/api/v3/{statement}/{symbol}"
    params = {"period": period, "apikey": os.getenv("FMP_API_KEY", "")}
    response = requests.get(base_url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()
    df = pd.DataFrame(data)

    fundamentals_dir = path_stocks / "fundamentals"
    fundamentals_dir.mkdir(parents=True, exist_ok=True)
    file_path = fundamentals_dir / f"{symbol}_{statement}_{period}.csv"
    df.to_csv(file_path, index=False)
    return df
