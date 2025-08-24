from pydoc import cli
import requests
import json
import os
from datetime import date
import urllib.request
from bs4 import BeautifulSoup
import re
import pandas as pd
import numpy as np
from datetime import *
from binance.client import Client


############################################################################################################################
############################## FUNCTIONS ##################################################################################
###########################################################################################################################

# Genera el token para los request de la API

class iol():
    def __init__(self, user_iol, psw_iol):
        self.user_iol= user_iol
        self.pass_iol = psw_iol

    def get_token(self, user_iol, psw_iol,):
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        body = {'username':  self.user_iol,'password':  self.pass_iol,'grant_type': GRANT_TYPE,}
        res = requests.post(URL_TOKEN, headers= {'Content-Type': 'application/x-www-form-urlencoded'}
        , data=body)
        tk= "Bearer "+res.json()['access_token']
        return tk

    def check_token(self, user_iol, psw_iol):
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}

        body = {
            'username':  self.user_iol,
            'password':  self.pass_iol,
            'grant_type': GRANT_TYPE,
        }
        res = requests.post(URL_TOKEN, headers= {'Content-Type': 'application/x-www-form-urlencoded',}
        , data=body)
        tk= "Barer "+res.json()['access_token']
        return tk

    # Genera una lista con los simbolos de las acciones pertenecientes a un panel

    def get_tickers_panel( self, instrumento, panel, pais, user_iol, psw_iol):

        auth= self.get_token(user_iol, psw_iol)

        simbolos=[]
        response = requests.get(f'{URL_API}/Cotizaciones/{instrumento}/{panel}/{pais}', headers= {'Content-Type': 'application/x-www-form-urlencoded', 'authorization': auth}
        )
        dict= json.loads(response.text)
        for i in dict["titulos"]:
            simbolos.append(i["simbolo"])
        return simbolos

    # Genera una tabla con los simbolod de un mercado particular

    def get_asset_iol(self, mercado, simbolo, user_iol):
        response = requests.get(
            f'{self.user_iol}/{mercado}/Titulos/{simbolo}', headers= {'Content-Type': 'application/x-www-form-urlencoded'}
        )
        return json.loads(response.text)

    def get_panel(self, instrumento, panel, pais, psw_iol, user_iol):
        auth= self.check_token(user_iol, psw_iol)

        response = requests.get(
            f'{URL_API}/Cotizaciones/{instrumento}/{panel}/{pais}', headers= {'Content-Type': 'application/x-www-form-urlencoded', 'authorization': auth}
        )
        res= json.loads(response.text)
        pd.DataFrame(res)

    # Genera una tabla con los datos historicos de una acción:

    def get_hist_data_iol(self, mercado, simbolo, desde, hasta,pais, path, psw_iol, user_iol):
        auth= self.get_token(user_iol, psw_iol)
        asset= "stock"
        response = requests.get(
            f'{URL_API}/{mercado}/Titulos/{simbolo}/Cotizacion/seriehistorica/{desde}/{hasta}/sinajustar', headers= {'Content-Type': 'application/x-www-form-urlencoded', 'authorization': auth}
        )
        res= response.json()
        asset= "stocks"
        df = pd.DataFrame(res)
        df= df.iloc[:,:12]
        df.columns= ["close", "var", "open", "max", "min", "dateTime","trend","opA" ,"pClose", "tradeQ", "volume", "meanp"]
        df= df.drop(columns=['trend', 'tradeQ','opA', 'pClose', 'meanp'], axis=1)
        df['dateTime']=df['dateTime'].str.slice(0, 10)
        df["dateTime"] = pd.to_datetime(df.dateTime)
        df=df[["close", "var", "open", "max", "min", "dateTime", "volume"]]
        df['ticker']= (simbolo)
        df["country"]= (pais)
        df["asset"]= asset
        df.set_index('dateTime', inplace=True)
    
        return df

    # Genera una base de datos con las acciones de Argentina o EEUU

    def get_DB_iol(self, instrumento, mercado,panel, pais,   desde,  path, user_iol, pass_iol):
        activos=[]
        simbolos=  self.get_tickers_panel(instrumento="acciones", pais=pais, panel=panel,user_iol= user_iol, psw_iol= pass_iol)
        for i in simbolos:
            if not str(i+".csv") in os.listdir(path):
                try:
                    print(i)
                    df= self.get_hist_data_iol(mercado= mercado,simbolo= i, desde=desde, hasta= date.today(),  pais= pais, path=path, psw_iol=pass_iol, user_iol= user_iol)
                    df.to_csv(path+i+".csv")

                except Exception as e:
                    print('error:', e)
                except:
                    pass

class binance:
    """Wrapper simplificado del cliente de Binance.

    Durante la inicialización se intenta crear una instancia de
    :class:`binance.client.Client`. Si la conexión falla (por ejemplo por
    falta de acceso a Internet), se captura la excepción para evitar que la
    aplicación se detenga.
    """

    def __init__(self):
        try:
            self.client = Client()
        except Exception as exc:  # pragma: no cover - solamente se ejecuta si falla la conexión
            self.client = None
            print(f"Unable to initialize Binance client: {exc}")
        
       
    # Genera una lista de cryptomonedas diosponibles en Binance

    def get_crypto_tickers(self):
        client= self.client
        holc= pd.DataFrame(client.get_ticker())
        Orderbook = pd.DataFrame(client.get_orderbook_tickers())
        merged_inner = pd.merge(left=holc, right=Orderbook, left_on='symbol', right_on='symbol')
        return merged_inner

    # Genera una tabla con datos historicos de una cryptomoneda, cada 5min

    def GetHistoricalData_crypto(self,sinceThisDate, par):
        client= self.client
        untilThisDate = datetime.now()
        candle = client.get_historical_klines(par, Client.KLINE_INTERVAL_5MINUTE, sinceThisDate, str(untilThisDate))
        df = pd.DataFrame(candle, columns=['dateTime', 'open', 'max', 'min', 'close', 'volume', 'closeTime', 'quoteAssetVolume', 'numberOfTrades', 'takerBuyBaseVol', 'takerBuyQuoteVol', 'ignore'])
        df['token']=(par)
        df['asset']= "cryptocurrency"
        df.dateTime = pd.to_datetime(df.dateTime, unit='ms')
        df.set_index('dateTime', inplace=True)
        return(df)

                    
    def get_db_crypto(self,path, since):
        try:
            li=[]
            dates=[]
            df=self.get_crypto_tickers()
            list=df['symbol']
            for i in list:
                if not ((str(i)+".csv")) in os.listdir(path):
                    print(i)
                    cur=self.GetHistoricalData_crypto(since, str(i))
                    print("saving")
                    cur.to_csv(path+str(i)+".csv")
                    print("ok")
                    li.append(i)
        
            if len(li)==0:
                updated= False
                print("DB ok")
                return updated
            else:
                updated= True
        except:
            pass



# Scraper de Wikipedia que devuelve una lista con los simbolos de los activos del mercado solicitado

def get_stocks_wiki(Market):
    if (Market== "s&p"):
        Stocks_tab=pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies", skiprows=1, header=0)[0]
        Stocks_tab=pd.DataFrame(Stocks_tab)
        Stocks_tab.columns=["Sym","Name","Reports", "Sector", "Sub_sector","State", "date added", "CIK", "Founded"]
    elif (Market== "dow jones"):
        Stocks_tab=pd.read_html("https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average", skiprows=1, header=0)[1]
        Stocks_tab=pd.DataFrame(Stocks_tab)
        Stocks_tab.columns=["Name","exchange","Sym", "Sector", "Date added","Notes", "%index"]
    else:
        Stocks_tab=pd.read_html("https://en.wikipedia.org/wiki/Nasdaq-100", skiprows=1, header=0)[3]
        Stocks_tab=pd.DataFrame(Stocks_tab)
        Stocks_tab.columns=["Name","Sym", "Sector", "SubSector"]

    Syms=[]
    Sym_esp=Stocks_tab["Sym"]
    for i in Sym_esp.items():
        text=(i[1])
        Syms.append(text)
    
    Name=[]
    Name_esp=Stocks_tab["Name"]
    for i in Name_esp.items():
        text=(i[1])
        text=text.replace(" ","-")
        Name.append(text)

    Sector=[] 
    Sectors=Stocks_tab["Sector"]
    for i in Sectors.items():
        text=(i[1])
        Sector.append(text)
        
    return Syms, Name, Sector

# Devuelve una tabla con el simbolo de las acciones de EEUU, su nombre y sector

def get_total_stocks():
    samp= get_stocks_wiki("s&p")
    d_jones= get_stocks_wiki("dow jones")
    nasdaq= get_stocks_wiki("nasdaq")
    Syms=samp[0]+nasdaq[0]+d_jones[0]
    Names=samp[1]+nasdaq[1]+d_jones[1]
    Sector=samp[2]+nasdaq[2]+d_jones[2]
    return pd.DataFrame(zip(Syms, Names, Sector), columns=["Syms", "Name", "Sector"])

# Inicializa la lista de acciones para evitar depender de una variable externa "lis".
try:
    lis = get_total_stocks()
except Exception:
    # En caso de que falle el request a Wikipedia se crea un DataFrame vacío
    lis = pd.DataFrame(columns=["Syms", "Name", "Sector"])

# Scrapper de investing que devuelve una tabla con los ultimos datos de los activos pedidos

def get_assets(type_, name_, pathDB, rows=1, page=1):
    """Descarga datos históricos de *Investing*.

    Se agregaron validaciones HTML para manejar cambios en la estructura de la
    página y se permite paginar y obtener múltiples filas por petición.

    Parameters
    ----------
    type_ : str
        Tipo de activo (ej. "commodities", "indices").
    name_ : str
        Nombre del activo utilizado en la URL de Investing.
    pathDB : str
        Ruta donde se almacenará el CSV resultante.
    rows : int, optional
        Cantidad de filas a descargar por petición. Por defecto 1.
    page : int, optional
        Página a solicitar en Investing. Por defecto 1.
    """

    print(name_)

    # Determina las columnas en función del tipo de activo
    if type_ == "currencies" or type_ == "rates-bonds":
        columns = ["dateTime", "Price", "Open", "High", "Low", "Var"]
    else:
        columns = ["dateTime", "Close", "Open", "Max", "Min", "Vol", "Var"]

    # Para acciones se intenta mapear el símbolo al nombre utilizando la lista
    if type_ == "equities":
        try:
            namepath = (
                lis.loc[lis["Syms"] == name_, "Name"].iloc[0]
            ).lower()
        except Exception:
            namepath = name_.lower()
    else:
        namepath = name_

    url = f"https://es.investing.com/{type_}/{namepath}-historical-data"
    if page > 1:
        url = f"{url}?p={page}"

    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req) as response:
        raw = response.read()

    soup = BeautifulSoup(raw, "lxml")

    # Validación de la estructura HTML
    table_html = soup.find("table")
    if table_html is None:
        raise ValueError("No se encontró la tabla de datos en la página")

    tbody = table_html.find("tbody")
    if tbody is None:
        raise ValueError("La tabla no contiene un <tbody> con datos")

    rows_html = tbody.find_all("tr")
    if not rows_html:
        raise ValueError("No se encontraron filas dentro de la tabla")

    # Procesa las filas solicitadas
    data = []
    for tr in rows_html[:rows]:
        values = []
        for td in tr.find_all("td"):
            value = td.get_text(strip=True)
            value = (
                value.replace("K", "000")
                .replace("M", "000000")
                .replace(",", ".")
            )
            values.append(value)
        if values:
            data.append(values)

    df = pd.DataFrame(data, columns=columns[: len(data[0])])
    df["asset"] = str(type_)

    # Normaliza fechas y establece el índice
    df["dateTime"] = pd.to_datetime(
        df["dateTime"].str.replace(".", "/"), dayfirst=True, errors="coerce"
    )
    df.set_index("dateTime", inplace=True)

    if pathDB is not None:
        df.to_csv(os.path.join(pathDB, f"{name_}.csv"))

    return df
# Función para actualizar la BD


def act_db_csv(path_crypto, path_stocks, path_indexes, path_commodities, path_currencies, path_macro, pass_iol, user_iol):
    for path in (path_crypto, path_stocks, path_indexes, path_commodities, path_currencies, path_macro):
        path_str = str(path)
        acciones = bool(re.search("acciones", path_str))
        commodities = bool(re.search("commodities", path_str))
        crypto = bool(re.search("crypto", path_str))
        divisas = bool(re.search("divisas", path_str))
        indices = bool(re.search("indices", path_str))
        macro = bool(re.search("macro", path_str))
        api_iol = iol(pass_iol, pass_iol)
        api_binance = binance()
        for file in os.listdir(path_str):
            df=pd.read_csv(path+file)
            name =  file.split('.')[0]
            print(name)
            date= max(pd.to_datetime(df.index()))
            if crypto == True:
                df1=pd.read_csv(path_crypto+name+".csv")
                curr=api_binance.GetHistoricalData_crypto(str(date), name)
                df1=df1.set_index('dateTime')
                df2=pd.concat([df1, curr], axis=0)
                print("saving")
                df2.to_csv(path_crypto+name+".csv")
                print(name+"ok")
            elif commodities == True:
                try:
                    df1 = pd.read_csv(path_commodities + name + ".csv")
                    curr = get_assets("commodities", name, path_commodities, rows=50)
                    mask = (curr['dateTime'] > date)
                    curr = curr.loc[mask]
                    df1 = df1.set_index('dateTime')
                    df2 = pd.concat([df1, curr], axis=0)
                    print("saving")
                    df2.to_csv(path_commodities + name + ".csv")
                    print(name + "ok")
                except Exception:
                    df1 = pd.read_csv(path_commodities + name + ".csv")
                    curr = get_assets("commodities", name, path_commodities, rows=50)
                    mask = (curr['dateTime'] > date)
                    curr = curr.loc[mask]
                    df1 = df1.set_index('dateTime')
                    df2 = pd.concat([df1, curr], axis=0)
                    print("saving")
                    df2.to_csv(path_commodities + name + ".csv")
                    print(name + "ok")
            elif indices == True:
                try:
                    df1 = pd.read_csv(path_indexes + name + ".csv")
                    curr = get_assets("indices", name, path_indexes, rows=50)
                    mask = (curr['dateTime'] > date)
                    curr = curr.loc[mask]
                    df1 = df1.set_index('dateTime')
                    df2 = pd.concat([df1, curr], axis=0)
                    print("saving")
                    df2.to_csv(path_indexes + name + ".csv")
                    print(name + "ok")
                except Exception:
                    df1 = pd.read_csv(path_indexes + name + ".csv")
                    curr = get_assets("indices", name, path_indexes, rows=50)
                    mask = (curr['dateTime'] > date)
                    curr = curr.loc[mask]
                    df1 = df1.set_index('dateTime')
                    df2 = pd.concat([df1, curr], axis=0)
                    print("saving")
                    df2.to_csv(path_indexes + name + ".csv")
                    print(name + "ok")
            elif divisas == True:
                try:
                    df1 = pd.read_csv(path_currencies + name + ".csv")
                    curr = get_assets("currencies", name, path_currencies, rows=50)
                    mask = (curr['dateTime'] > date)
                    curr = curr.loc[mask]
                    df1 = df1.set_index('dateTime')
                    df2 = pd.concat([df1, curr], axis=0)
                    print("saving")
                    df2.to_csv(path_currencies + name + ".csv")
                    print(name + "ok")
                except Exception:
                    df1 = pd.read_csv(path_currencies + name + ".csv")
                    curr = get_assets("currencies", name, path_currencies, rows=50)
                    mask = (curr['dateTime'] > date)
                    curr = curr.loc[mask]
                    df1 = df1.set_index('dateTime')
                    df2 = pd.concat([df1, curr], axis=0)
                    print("saving")
                    df2.to_csv(path_currencies + name + ".csv")
                    print(name + "ok")
            elif macro == True:
                try:
                    df1 = pd.read_csv(path_macro + name + ".csv")
                    curr = get_assets("rates-bonds", name, path_macro, rows=50)
                    mask = (curr['dateTime'] > date)
                    curr = curr.loc[mask]
                    df1 = df1.set_index('dateTime')
                    df2 = pd.concat([df1, curr], axis=0)
                    print("saving")
                    df2.to_csv(path_macro + name + ".csv")
                    print(name + "ok")
                except Exception:
                    df1 = pd.read_csv(path_macro + name + ".csv")
                    curr = get_assets("rates-bonds", name, path_macro, rows=50)
                    mask = (curr['dateTime'] > date)
                    curr = curr.loc[mask]
                    df1 = df1.set_index('dateTime')
                    df2 = pd.concat([df1, curr], axis=0)
                    print("saving")
                    df2.to_csv(path_macro + name + ".csv")
                    print(name + "ok")
            elif acciones == True:
                try:
                    df1=pd.read_csv(path_stocks+name+".csv")
                    curr=api_iol.get_hist_data_iol('bcBA', name, date, date.today(),'argentina', path_stocks, psw_iol=pass_iol, user_iol=user_iol)
                    df1=df1.set_index('dateTime')
                    df2=pd.concat([df1, curr], axis=0)
                    print("saving")
                    df2.to_csv(path_stocks+name+".csv")
                    print(name+"ok")
                except:
                    df1=pd.read_csv(path_stocks+name+".csv")
                    curr=api_iol.get_hist_data_iol('bcBA', name, date, date.today(),'estados_unidos', path_stocks, psw_iol=pass_iol, user_iol=user_iol)
                    df1=df1.set_index('dateTime')
                    df2=pd.concat([df1, curr], axis=0)
                    print("saving")
                    df2.to_csv(path_stocks+name+".csv")
                    print(name+"ok")
    print(datetime.now())
            
