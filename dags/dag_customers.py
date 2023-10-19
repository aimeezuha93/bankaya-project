import sys
from airflow import DAG
from datetime import datetime, timedelta
from airflow.decorators import task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.utils.helpers import chain
from libs.database_handler import db_handler, load_dwh
from libs.extraction import get_db_data, get_json_data
from libs.transformation import process_data

sys.path.insert(0, "/usr/local/airflow/dags")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 3, 16),
    "email": ["aimee.@company.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "dag_customers",
    default_args=default_args,
    catchup=False,
    schedule_interval=timedelta(hours=5),
) as dag:

    start = EmptyOperator(task_id="start")

    @task()
    def create_db_objects():
        db_handler()

    _create_db_objects = create_db_objects()

    @task_group()
    def get_data():
        @task()
        def extract_json_data():
            get_json_data()

        _extract_json_data = extract_json_data()

        @task()
        def extract_db_data():
            get_db_data()

        _extract_db_data = extract_db_data()

        chain[_extract_json_data, _extract_db_data]

    _get_data = get_data()

    @task()
    def transform_data():
        process_data()

    _transform_data = transform_data()

    @task()
    def load_data():
        load_dwh()

    _load_data = load_data()

    chain(
        _create_db_objects,
        _get_data,
        _transform_data,
        _load_data
    )
