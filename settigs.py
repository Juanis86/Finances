#########################################################################################################################
################################# PARAMETROS ############################################################################
#########################################################################################################################
import pathlib
import sys
import os
from dotenv import load_dotenv
load_dotenv()
import os
import pathlib

import pathlib

path = (pathlib.Path(__file__).absolute()).parent.parent


USER_IOL= os.getenv('USER_IOL')
PASS_IOL=os.getenv('PASS_IOL')
URL_TOKEN = os.getenv('URL_TOKEN')
GRANT_TYPE = os.getenv('GRANT_TYPE')
URL_API = os.getenv('URL_API')
# Elijo el formato a guardar (csv, sql o ambas)

save_sql= True
save_csv= True 

# Elijo los destinos para los distintos archivos en caso de guardar como csv

dataset= f'{path}/dataset'
path_stocks= f'{dataset}/acciones/'
path_crypto=f'{dataset}/crypto/'
path_commodities= f'{dataset}/commodities/'
path_indexes= f'{dataset}/indices/'
path_currencies= f'{dataset}/divisas/'
path_macro=f'{dataset}/macro/'
