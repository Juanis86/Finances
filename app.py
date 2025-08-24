from settings import *
from main_func import *
from coingecko_client import fetch_coins_markets

from global_stocks import get_price_series, save_series_to_csv

#Cargo las variables de entorno




if __name__== '__main__':
    start= datetime.now()
    create_DB= False
    IOL_Api= iol(USER_IOL, PASS_IOL)
    Binance_Api= binance()


    if create_DB== True:
            Binance_Api.get_db_crypto(path_crypto, "2018-01-01 00:00:00")# Genero la BD desde 2018 con timeframe de 5´
            fetch_coins_markets("usd", 250)
            IOL_Api.get_DB_iol("acciones", "bcBA", "panel general", "argentina", "2010-01-01",path_stocks, USER_IOL, PASS_IOL)
            IOL_Api.get_DB_iol("acciones", "nySE", "sp500", "estados_unidos", "2010-01-01",path_stocks, USER_IOL, PASS_IOL)
            Witi_oil = get_assets("commodities", "crude-oil", path_commodities, rows=50)
            Soybeans = get_assets("commodities", "us-soybeans", path_commodities, rows=50)
            Corn = get_assets("commodities", "us-corn", path_commodities, rows=50)
            Wheat = get_assets("commodities", "london-wheat", path_commodities, rows=50)
            Copper = get_assets("commodities", "copper", path_commodities, rows=50)
            Samp = get_assets("indices", "us-spx-500", path_indexes, rows=50)
            Dow_joes = get_assets("indices", "us-30", path_indexes, rows=50)
            Nasdaq = get_assets("indices", "nasdaq-composite", path_indexes, rows=50)
            Bovespa = get_assets("indices", "bovespa", path_indexes, rows=50)
            Shangai = get_assets("indices", "shanghai-composite", path_indexes, rows=50)
            Ibex = get_assets("indices", "spain-35", path_indexes, rows=50)
            uk_100 = get_assets("indices", "uk-100", path_indexes, rows=50)
            merval = get_assets("indices", "merv", path_indexes, rows=50)
            usd_ars = get_assets("currencies", "usd-ars", path_currencies, rows=50)
            usd_bz = get_assets("currencies", "usd-brl", path_currencies, rows=50)
          #  usd_10y= get_assets("rates-bonds","u.s.-10-year-bond-yield",path_macro, rows=50)
            print(start)
            print(datetime.now)

    else:
        act_db_csv(path_crypto,path_stocks, path_indexes, path_commodities, path_currencies, path_macro, pass_iol= PASS_IOL,user_iol= USER_IOL)
        fetch_coins_markets("usd", 250)
        # Actualización de las demás bases de datos
        tmp_macro = path_macro / "_tmp_ignore"
        os.makedirs(tmp_macro, exist_ok=True)
        act_db_csv(path_crypto, path_stocks, path_indexes, path_commodities, path_currencies, tmp_macro,
                   pass_iol=PASS_IOL, user_iol=USER_IOL)

        # Actualización de series macro
        for serie in MACRO_SERIES:
            file_path = path_macro / f"{serie['id']}.csv"
            if file_path.exists():
                df_existing = pd.read_csv(file_path, parse_dates=['date'])
                last_date = df_existing['date'].max()
                if serie['source'] == 'fred':
                    start_date = (last_date + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
                    end_date = date.today().strftime('%Y-%m-%d')
                else:
                    start_date = str(last_date.year + 1)
                    end_date = str(date.today().year)
            else:
                start_date = serie['start']
                end_date = date.today().strftime('%Y-%m-%d') if serie['source'] == 'fred' else str(date.today().year)

            fetch_series(serie['id'], start_date, end_date, serie['source'])
