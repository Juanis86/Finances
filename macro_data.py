import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

from settings import path_macro


def fetch_series(series_id: str, start: str, end: str, source: str) -> pd.DataFrame:
    """Fetch a macroeconomic time series from FRED or the World Bank.

    Parameters
    ----------
    series_id : str
        Identifier of the series in the chosen data source.
    start : str
        Start of the period to fetch. For FRED use ``YYYY-MM-DD`` format, for
        the World Bank use ``YYYY``.
    end : str
        End of the period to fetch. For FRED use ``YYYY-MM-DD`` format, for the
        World Bank use ``YYYY``.
    source : str
        Data source, either ``"fred"`` or ``"worldbank"``.

    Returns
    -------
    pandas.DataFrame
        Time series with two columns: ``date`` and ``value``. The data is also
        persisted as ``CSV`` within ``path_macro`` using ``series_id`` as file
        name. If a file already exists, new data is appended and duplicate dates
        are removed.
    """

    source = source.lower()

    if source == "fred":
        url = "https://api.stlouisfed.org/fred/series/observations"
        params = {
            "series_id": series_id,
            "observation_start": start,
            "observation_end": end,
            "api_key": os.getenv("FRED_API_KEY", ""),
            "file_type": "json",
        }
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        observations = response.json().get("observations", [])
        df = pd.DataFrame(observations)[["date", "value"]]
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df.dropna(subset=["date", "value"], inplace=True)

    elif source in {"worldbank", "wb"}:
        url = f"https://api.worldbank.org/v2/country/WLD/indicator/{series_id}"
        params = {"date": f"{start}:{end}", "format": "json", "per_page": 1000}
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        json_data = response.json()
        data = json_data[1] if len(json_data) > 1 else []
        df = pd.DataFrame([{"date": item["date"], "value": item["value"]} for item in data])
        df["date"] = pd.to_datetime(df["date"], format="%Y", errors="coerce")
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df.dropna(subset=["date", "value"], inplace=True)
        df.sort_values("date", inplace=True)

    else:
        raise ValueError("source must be 'fred' or 'worldbank'")

    path_macro.mkdir(parents=True, exist_ok=True)
    file_path = path_macro / f"{series_id}.csv"

    if file_path.exists():
        existing = pd.read_csv(file_path)
        if "date" in existing.columns:
            existing["date"] = pd.to_datetime(existing["date"])
        combined = pd.concat([existing, df], ignore_index=True)
        combined.drop_duplicates(subset="date", inplace=True)
        combined.sort_values("date", inplace=True)
        combined.to_csv(file_path, index=False)
        return combined

    df.to_csv(file_path, index=False)
    return df
