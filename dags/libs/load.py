import pandas as pd

from helpers.database_connection import get_engine

credentials = {
    "host": "bankaya-project-postgres-1",
    "user": "airflow",
    "password": "airflow",
    "database": "airflow",
    "port": "5432",
}

def put_data(tmp_path, **kwargs):
    engine = get_engine(**credentials)
    tmp_path = kwargs["ti"].xcom_pull(key="return_value", task_ids="transform_data")
    file_tmp = f"{tmp_path}/df_fact"
    df_fact = pd.read_pickle(file_tmp)
    df_fact.to_sql('sales', schema='facts', con=engine, if_exists='replace', index=False)
