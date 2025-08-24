# Finances
The code code creates a database of all the cryptocurrencies in the Binance DB, every 5 minutes, starting from the chosen date. Also, having a password and user, it creates a DB of Argentine shares and CEDEARs. Additionally, it collects macro, index and commodity data by scrapping. Finally, it incorporates analysis tools

## Variables de entorno

El proyecto utiliza varias APIs externas. Las claves deben definirse como
variables de entorno (por ejemplo en un archivo `.env`).

- `USER_IOL`, `PASS_IOL`, `URL_TOKEN`, `GRANT_TYPE`, `URL_API`: credenciales
  necesarias para interactuar con la API de IOL.
- `FRED_API_KEY`: clave para consultar series macroeconómicas desde FRED.
- `ALPHA_VANTAGE_API_KEY`: clave para descargar datos desde Alpha Vantage.

La librería `yfinance` no requiere autenticación.
