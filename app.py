from settings import *
from main_func import *
from coingecko_api import get_coin_list, get_historical_data
import os

# Cargo las variables de entorno


if __name__ == '__main__':
    start = datetime.now()
    create_DB = False
    source = os.getenv("CRYPTO_SOURCE", "binance").lower()

    IOL_Api = iol(USER_IOL, PASS_IOL)
    binance_api = binance() if source in ("binance", "both") else None

    if create_DB == True:
        if source in ("binance", "both") and binance_api is not None:
            binance_api.get_db_crypto(path_crypto, "2018-01-01 00:00:00")  # Genero la BD desde 2018 con timeframe de 5´
        if source in ("coingecko", "both"):
            coins = get_coin_list()
            for coin in coins["id"][:10]:
                try:
                    hist = get_historical_data(coin, "2018-01-01 00:00:00")
                    hist.to_csv(path_crypto / f"{coin}.csv")
                except Exception as exc:
                    print(f"{coin}: {exc}")
        IOL_Api.get_DB_iol("acciones", "bcBA", "panel general", "argentina", "2010-01-01", path_stocks, USER_IOL, PASS_IOL)
        IOL_Api.get_DB_iol("acciones", "nySE", "sp500", "estados_unidos", "2010-01-01", path_stocks, USER_IOL, PASS_IOL)
        Witi_oil = get_assets("commodities", "crude-oil", 1, path_commodities)
        Soybeans = get_assets("commodities", "us-soybeans", 1, path_commodities)
        Corn = get_assets("commodities", "us-corn", 1, path_commodities)
        Wheat = get_assets("commodities", "london-wheat", 1, path_commodities)
        Copper = get_assets("commodities", "copper", 1, path_commodities)
        Samp = get_assets("indices", "us-spx-500", 1, path_indexes)
        Dow_joes = get_assets("indices", "us-30", 1, path_indexes)
        Nasdaq = get_assets("indices", "nasdaq-composite", 0, path_indexes)
        Bovespa = get_assets("indices", "bovespa", 0, path_indexes)
        Shangai = get_assets("indices", "shanghai-composite", 0, path_indexes)
        Ibex = get_assets("indices", "spain-35", 1, path_indexes)
        uk_100 = get_assets("indices", "uk-100", 1, path_indexes)
        merval = get_assets("indices", "merv", 0, path_indexes)
        usd_ars = get_assets("currencies", "usd-ars", 0, path_currencies)
        usd_bz = get_assets("currencies", "usd-brl", 1, path_currencies)
        # usd_10y= get_assets("rates-bonds","u.s.-10-year-bond-yield",0,path_macro)
        print(start)
        print(datetime.now)
    else:
        act_db_csv(
            path_crypto,
            path_stocks,
            path_indexes,
            path_commodities,
            path_currencies,
            path_macro,
            pass_iol=PASS_IOL,
            user_iol=USER_IOL,
        )
