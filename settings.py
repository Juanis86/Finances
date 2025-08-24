#########################################################################################################################
################################# PARAMETROS ############################################################################
#########################################################################################################################

"""Aplicación settings.

Este módulo centraliza la carga de variables de entorno y la creación de
directorios necesarios para almacenar los datos descargados.
"""

import os
from pathlib import Path

from dotenv import load_dotenv


# Cargar variables del archivo `.env` si existe
load_dotenv()


# Variables de entorno utilizadas por la aplicación
USER_IOL = os.getenv("USER_IOL")
PASS_IOL = os.getenv("PASS_IOL")
URL_TOKEN = os.getenv("URL_TOKEN")
GRANT_TYPE = os.getenv("GRANT_TYPE")
URL_API = os.getenv("URL_API")


# Elijo el formato a guardar (csv, sql o ambas)
save_sql = True
save_csv = True


# Directorios para almacenar los distintos tipos de datos
BASE_PATH = Path(__file__).resolve().parent
dataset = BASE_PATH / "dataset"
path_stocks = dataset / "acciones"
path_bonds = dataset / "bonos"
path_options = dataset / "opciones"
path_crypto = dataset / "crypto"
path_commodities = dataset / "commodities"
path_indexes = dataset / "indices"
path_currencies = dataset / "divisas"
path_macro = dataset / "macro"


# Crear los directorios en caso de que no existan
for directory in (
    path_stocks,
    path_bonds,
    path_options,
    path_crypto,
    path_commodities,
    path_indexes,
    path_currencies,
    path_macro,
):
    os.makedirs(directory, exist_ok=True)

