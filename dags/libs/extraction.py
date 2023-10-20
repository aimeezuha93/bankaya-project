import json
import glob
from pathlib import Path
from sqlalchemy import text
import pandas as pd
from helpers.logging import get_logger
from helpers.database_connection import get_engine
from helpers.utils import get_tmp_path_files, save_df_file
from helpers.queries import *


logger = get_logger()
credentials = {
    "host": "bankaya-project-pgadmin4-1",
    "user": "airflow",
    "password": "airflow",
    "database": "airflow",
    "port": "5432",
}


def get_db_data(dag_main_file):
    engine = get_engine(**credentials)
    df_customer = pd.read_sql(sql=text(query_customers), con=engine)
    df_items = pd.read_sql(sql=text(query_items), con=engine)
    df_stores = pd.read_sql(sql=text(query_stores), con=engine)
    
    path_tmp = f"{get_tmp_path_files(dag_main_file)}{Path(dag_main_file).stem}"
    save_df_file(path_tmp, "df_costumers", df_customer)
    save_df_file(path_tmp, "df_items", df_items)
    save_df_file(path_tmp, "df_stores", df_stores)
    return  path_tmp


def get_json_data(dag_main_file):
    json_files = glob.glob('dags/resources/raw/*', recursive=True)
    df = pd.DataFrame()

    for file in json_files:
        with open(file) as json_file:
            json_text = json.load(json_file)
            df = pd.concat(
                        [df, pd.json_normalize(json_text,record_path=['items_bought'], meta=[ 
                        'customer_id', 'store_id', 'creation_timestamp'])], axis=0, ignore_index=True
                    )
    path_tmp = f"{get_tmp_path_files(dag_main_file)}{Path(dag_main_file).stem}"
    save_df_file(path_tmp, "df_orders", df)
    return path_tmp
