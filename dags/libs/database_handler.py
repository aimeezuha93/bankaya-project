import sys
from sqlalchemy import text
from helpers.logging import get_logger
from helpers.database_connection import get_engine
from helpers.queries import *


sys.path.insert(0, "/usr/local/airflow/dags")

logger = get_logger()
credentials = {
    "host": "bankaya-project-postgres-1",
    "user": "airflow",
    "password": "airflow",
    "database": "airflow",
    "port": "5432",
}


def db_handler():
    create_schemas()
    create_tables()


def create_schemas():
    engine = get_engine(**credentials)

    with engine.begin() as conn:
        try:
            conn.execute(text(query_schema_facts))
            conn.execute(text("COMMIT;"))
        except Exception as e:
            logger.info(f"Postgres Error: {e}")
        finally:
            conn.close()


def create_tables():
    engine = get_engine(**credentials)
    with engine.begin() as conn:
        try:
            logger.info("Creating fact tables.")
            conn.execute(text(query_fact_table))
            conn.execute(text("COMMIT;"))
        except Exception as e:
            logger.info(f"Postgres Error: {e}")
        finally:
            conn.close()


def load_dwh():
    engine = get_engine(**credentials)
    with engine.begin() as conn:
        try:
            logger.info("Inserting fact table.")
            conn.execute(text(insert_dim_hist))
            conn.execute(text("COMMIT;"))
        except Exception as e:
            logger.info(f"Postgres Error: {e}")
        finally:
            conn.close()
