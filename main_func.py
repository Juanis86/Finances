from pydoc import cli
import requests
import json
import os
from datetime import date, datetime, timedelta
import urllib.request
from bs4 import BeautifulSoup
import re
import pandas as pd
import numpy as np
from binance.client import Client

# Variables de configuración de la API de IOL
from settings import URL_TOKEN, URL_API, GRANT_TYPE


############################################################################################################################
############################## FUNCTIONS ##################################################################################
###########################################################################################################################

# Genera el token para los request de la API

class iol:
    def __init__(self, user_iol, psw_iol):
        self.user_iol = user_iol
        self.pass_iol = psw_iol
        self.session = requests.Session()
        self.token = None
        self.token_expires_at = datetime.min

    def get_token(self):
        """Obtener un nuevo token de autenticación."""
        body = {
            'username': self.user_iol,
            'password': self.pass_iol,
            'grant_type': GRANT_TYPE,
        }
        response = self.session.post(
            URL_TOKEN,
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
            data=body,
        )
        response.raise_for_status()
        data = response.json()
        self.token = f"Bearer {data['access_token']}"
        expires_in = data.get('expires_in', 0)
        self.token_expires_at = datetime.utcnow() + timedelta(seconds=expires_in)
        return self.token

    def refresh_token(self):
        """Renovar el token si expiró."""
        if self.token is None or datetime.utcnow() >= self.token_expires_at - timedelta(seconds=60):
            self.get_token()
        return self.token

    # Genera una lista con los simbolos de las acciones pertenecientes a un panel
    def get_tickers_panel(self, instrumento, panel, pais, user_iol=None, psw_iol=None):
        auth = self.refresh_token()
        response = self.session.get(
            f'{URL_API}/Cotizaciones/{instrumento}/{panel}/{pais}',
            headers={'Content-Type': 'application/x-www-form-urlencoded', 'authorization': auth},
        )
        response.raise_for_status()
        data = response.json()
        simbolos = [i["simbolo"] for i in data.get("titulos", [])]
        return simbolos

    # Genera una tabla con los simbolos de un mercado particular
    def get_asset_iol(self, mercado, simbolo, user_iol=None):
        auth = self.refresh_token()
        response = self.session.get(
            f'{URL_API}/{mercado}/Titulos/{simbolo}',
            headers={'Content-Type': 'application/x-www-form-urlencoded', 'authorization': auth},
        )
        response.raise_for_status()
        return response.json()

    def get_panel(self, instrumento, panel, pais, psw_iol=None, user_iol=None):
        auth = self.refresh_token()
        response = self.session.get(
            f'{URL_API}/Cotizaciones/{instrumento}/{panel}/{pais}',
            headers={'Content-Type': 'application/x-www-form-urlencoded', 'authorization': auth},
        )
        response.raise_for_status()
        res = response.json()
        pd.DataFrame(res)

    # Genera una tabla con los datos historicos de una acción:
    def get_hist_data_iol(self, mercado, simbolo, desde, hasta, pais, path, psw_iol=None, user_iol=None):
        auth = self.refresh_token()
        response = self.session.get(
            f'{URL_API}/{mercado}/Titulos/{simbolo}/Cotizacion/seriehistorica/{desde}/{hasta}/sinajustar',
            headers={'Content-Type': 'application/x-www-form-urlencoded', 'authorization': auth},
        )
        response.raise_for_status()
        res = response.json()
        df = pd.DataFrame(res)
        df = df.iloc[:, :12]
        df.columns = ["close", "var", "open", "max", "min", "dateTime", "trend", "opA", "pClose", "tradeQ", "volume", "meanp"]
        df = df.drop(columns=['trend', 'tradeQ', 'opA', 'pClose', 'meanp'], axis=1)
        df['dateTime'] = df['dateTime'].str.slice(0, 10)
        df["dateTime"] = pd.to_datetime(df.dateTime)
        df = df[["close", "var", "open", "max", "min", "dateTime", "volume"]]
        df['ticker'] = simbolo
        df["country"] = pais
        df["asset"] = "stocks"
        df.set_index('dateTime', inplace=True)
        return df

    # Datos intradiarios de un activo
    def get_intraday_iol(self, mercado, simbolo):
        auth = self.refresh_token()
        response = self.session.get(
            f'{URL_API}/{mercado}/Titulos/{simbolo}/Cotizacion/Intradiaria',
            headers={'Content-Type': 'application/x-www-form-urlencoded', 'authorization': auth},
        )
        response.raise_for_status()
        return response.json()

    # Datos fundamentales de un activo
    def get_fundamentals_iol(self, mercado, simbolo):
        auth = self.refresh_token()
        response = self.session.get(
            f'{URL_API}/{mercado}/Titulos/{simbolo}/Fundamentals',
            headers={'Content-Type': 'application/x-www-form-urlencoded', 'authorization': auth},
        )
        response.raise_for_status()
        return response.json()

    # Genera una base de datos con las acciones de Argentina o EEUU
    def get_DB_iol(self, instrumento, mercado, panel, pais, desde, path, user_iol=None, pass_iol=None):
        activos = []
        simbolos = self.get_tickers_panel(instrumento="acciones", pais=pais, panel=panel, user_iol=user_iol, psw_iol=pass_iol)
        for i in simbolos:
            if not str(i + ".csv") in os.listdir(path):
                try:
                    print(i)
                    df = self.get_hist_data_iol(
                        mercado=mercado,
                        simbolo=i,
                        desde=desde,
                        hasta=date.today(),
                        pais=pais,
                        path=path,
                        psw_iol=pass_iol,
                        user_iol=user_iol,
                    )
                    df.to_csv(path + i + ".csv")
                except requests.HTTPError as e:
                    print('error:', e)
                except Exception as e:
                    print('error:', e)

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

# Scrapper de investing que devuelve una tabla con los ultimos datos de los activos pedidos

def get_assets(type_,name_, cod, pathDB):
    print(name_)
    if type_=="currencies" or type_=="rates-bonds":
        columns=["dateTime", "Price", "Open","High", "Low", "Var"]
    else:
        columns=["dateTime", "Close", "Open","Max", "Min", "Vol", "Var"]
    if type_=="equities":
        namepath=(lis.loc[lis["Syms"]==name_, "Name"]).unique()[1]
        namepath=name_.lower()
    else:
        namepath=name_
    path= "https://es.investing.com/"+type_+"/"+ name_+"-historical-data"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    req = urllib.request.Request(path, headers=headers)
    with urllib.request.urlopen(req) as response:
        raw = response.read()
    soup = BeautifulSoup(raw, "lxml")
    table=[]
    df=[]
    row_=soup.find_all({"tbody":"tr"})[cod]
    row_=row_.contents
    for i in row_:
        row=[]
        for n in i:
            value=str(re.findall('>.*<',str(n)))
            value=value.replace("<",'')
            value=value.replace(">",'')
            value=value.replace("K","000")
            value=value.replace(",",".")
            value=value.replace('[', '')
            value=value.replace(']', '')
            value=value.replace("'","")
            if value!='[]' and value!="":
                try:
                    row.append(float(value))
                except:
                    row.append(value)
        if len(row)>0:
            table.append(row)
    for j in table:
        if j !=[""]:
            df.append(j)
    df= pd.DataFrame(df)
    df['asset']=str(type_)
    df['dateTime']= df.iloc[:,0].apply(lambda x: (x.split('"')[1]).replace('.','/'))
    df.dateTime = pd.to_datetime(df.dateTime, format='%d/%m/%Y')
    df.set_index('dateTime', inplace=True)
    df.to_csv(pathDB+(name_)+".csv")
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
                    df1=pd.read_csv(path_commodities+name+".csv")
                    curr= get_assets("commodities", name,1,path_commodities)
                    mask = (curr['dateTime'] > date)
                    curr=curr.loc[mask]
                    df1=df1.set_index('dateTime')
                    df2=pd.concat([df1, curr], axis=0)
                    print("saving")
                    df2.to_csv(path_commodities+name+".csv")
                    print(name+"ok")
                except:
                    df1=pd.read_csv(path_commodities+name+".csv")
                    curr= get_assets("commodities", name,0,path_commodities)
                    mask = (curr['dateTime'] > date)
                    curr=curr.loc[mask]
                    df1=df1.set_index('dateTime')
                    df2=pd.concat([df1, curr], axis=0)
                    print("saving")
                    df2.to_csv(path_commodities+name+".csv")
                    print(name+"ok")
            elif indices == True:
                try:
                    df1=pd.read_csv(path_indexes+name+".csv")
                    curr= get_assets("commodities", name,1,path_indexes)
                    mask = (curr['dateTime'] > date)
                    curr=curr.loc[mask]
                    df1=df1.set_index('dateTime')
                    df2=pd.concat([df1, curr], axis=0)
                    print("saving")
                    df2.to_csv(path_indexes+name+".csv")
                    print(name+"ok")
                except:
                    df1=pd.read_csv(path_indexes+name+".csv")
                    curr= get_assets("commodities", name,0,path_indexes)
                    mask = (curr['dateTime'] > date)
                    curr=curr.loc[mask]
                    df1=df1.set_index('dateTime')
                    df2=pd.concat([df1, curr], axis=0)
                    print("saving")
                    df2.to_csv(path_indexes+name+".csv")
                    print(name+"ok")
            elif divisas == True:
                try:
                    df1=pd.read_csv(path_currencies+name+".csv")
                    curr= get_assets("commodities", name,1,path_currencies)
                    mask = (curr['dateTime'] > date)
                    curr=curr.loc[mask]
                    df1=df1.set_index('dateTime')
                    df2=pd.concat([df1, curr], axis=0)
                    print("saving")
                    df2.to_csv(path_currencies+name+".csv")
                    print(name+"ok")
                except:
                    df1=pd.read_csv(path_currencies+name+".csv")
                    curr= get_assets("commodities", name,0,path_currencies)
                    mask = (curr['dateTime'] > date)
                    curr=curr.loc[mask]
                    df1=df1.set_index('dateTime')
                    df2=pd.concat([df1, curr], axis=0)
                    print("saving")
                    df2.to_csv(path_currencies+name+".csv")
                    print(name+"ok")
            elif macro==True:
                try:
                    df1=pd.read_csv(path_macro+name+".csv")
                    curr= get_assets("commodities", name,1,path_macro)
                    mask = (curr['dateTime'] > date)
                    curr=curr.loc[mask]
                    df1=df1.set_index('dateTime')
                    df2=pd.concat([df1, curr], axis=0)
                    print("saving")
                    df2.to_csv(path_macro+name+".csv")
                    print(name+"ok")
                except:
                    df1=pd.read_csv(path_macro+name+".csv")
                    curr= get_assets("commodities", name,0,path_macro)
                    mask = (curr['dateTime'] > date)
                    curr=curr.loc[mask]
                    df1=df1.set_index('dateTime')
                    df2=pd.concat([df1, curr], axis=0)
                    print("saving")
                    df2.to_csv(path_macro+name+".csv")
                    print(name+"ok")
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
            
