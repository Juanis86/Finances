import os
import pandas as pd
from fredapi import Fred


def get_series(series_id: str, start_date: str) -> pd.DataFrame:
    """Descarga una serie de FRED y la devuelve como DataFrame.

    Parameters
    ----------
    series_id: str
        Identificador de la serie en FRED (por ejemplo, ``"DGS10"``).
    start_date: str
        Fecha inicial en formato ``"AAAA-MM-DD"``.

    Returns
    -------
    pandas.DataFrame
        Serie temporal con índice de fechas y una columna ``value``.
    """
    api_key = os.getenv("FRED_API_KEY")
    fred = Fred(api_key=api_key)
    data = fred.get_series(series_id, observation_start=start_date)
    df = pd.DataFrame(data, columns=["value"])
    df.index.name = "date"
    return df
