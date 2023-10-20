import pandas as pd
from pathlib import Path
from helpers.utils import get_tmp_path_files, save_df_file

def process_data(dag_main_file, **kwargs):
    oltp_tmp_paths = kwargs["ti"].xcom_pull(key="return_value", task_ids="extract_db_data")
    json_tmp_paths = kwargs["ti"].xcom_pull(key="return_value", task_ids="extract_json_data")
    oltp_tmp_customer = f"{oltp_tmp_paths}/df_costumers"
    oltp_tmp_item = f"{oltp_tmp_paths}/df_items"
    oltp_tmp_store = f"{oltp_tmp_paths}/df_stores"
    json_tmp = f"{oltp_tmp_paths}/df_orders"
    df_customers = pd.read_pickle(oltp_tmp_customer)
    df_items = pd.read_pickle(oltp_tmp_item)
    df_stores = pd.read_pickle(oltp_tmp_store)
    df_orders = pd.read_pickle(json_tmp)
    cols_orders = [
        "customer_id",
        "store_id",
        "creation_timestamp",
        "item_id",
        "quatity",
        "total_price",
    ]
    df_orders = df_orders[cols_orders]
    df_fact = df_orders.merge(df_customers[['customer_id', 'rfc']], on='customer_id', how='inner')
    df_fact = df_fact.merge(df_items[['item_id', 'name']], on='item_id', how='inner')
    df_fact = df_fact.merge(df_stores[['store_id', 'name']], on='store_id', how='inner')
    df_fact = df_fact.drop(columns=['customer_id', 'store_id'])
    df_fact = df_fact.groupby(['rfc','creation_timestamp', 'name_y', 'item_id', 'name_x'],as_index=False).agg({'quatity':'sum','total_price':'sum'})
    df_fact['creation_timestamp'] = df_fact['creation_timestamp'].str.replace('T',' ')
    df_fact['creation_timestamp'] = df_fact['creation_timestamp'].str.replace('Z','')
    df_fact['creation_timestamp'] = pd.to_datetime(df_fact['creation_timestamp'], utc=True)
    df_fact['creation_timestamp'] = df_fact['creation_timestamp'].dt.tz_convert('America/Mexico_City').dt.date
    df_fact['rfc'] = df_fact['rfc'].str.upper()
    df_fact['name_x'] = df_fact['name_x'].str.upper()
    df_fact['name_y'] = df_fact['name_y'].str.upper()
    df_fact['name_x'].replace(regex=True, inplace=True, to_replace=r'[^a-zA-Z0-9\s]', value=r'')
    df_fact['name_y'].replace(regex=True, inplace=True, to_replace=r'[^a-zA-Z0-9\s]', value=r'')
    df_fact.rename(columns = {'rfc':'customer_rfc', 'name_x':'item_name', 'quatity':'item_quantity_bought', 'name_y':'store_name', 'total_price':'total_bought', 'creation_timestamp':'purchase_date'}, inplace = True)
    cols_fact = [
        "customer_rfc",
        "item_name",
        "item_quantity_bought",
        "item_id",
        "store_name",
        "total_bought",
        "purchase_date"
    ]
    df_fact = df_fact[cols_fact]
    path_tmp = f"{get_tmp_path_files(dag_main_file)}{Path(dag_main_file).stem}"
    save_df_file(path_tmp, "df_fact", df_fact)
    return path_tmp
