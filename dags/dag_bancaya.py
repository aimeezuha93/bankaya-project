from airflow import DAG
from datetime import datetime, timedelta
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.utils.helpers import chain
from libs.database_handler import db_handler
from libs.extraction import get_db_data, get_json_data
from libs.transformation import process_data
from libs.load import put_data


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 3, 16),
    "email": ["aimee.@company.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "provide_context": True
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

    @task()
    def extract_json_data(dag_file_name):
        get_json_data(dag_file_name)

    _extract_json_data = extract_json_data(__file__)

    @task()
    def extract_db_data(dag_file_name):
        get_db_data(dag_file_name)

    _extract_db_data = extract_db_data(__file__)

    @task()
    def transform_data(dag_file_name):
        process_data(dag_file_name)

    _transform_data = transform_data(__file__)

    @task()
    def load_data():
        put_data()

    _load_data = load_data()

    chain(
        start,
        _create_db_objects,
        _extract_json_data,
        _extract_db_data,
        _transform_data,
        _load_data
    )
