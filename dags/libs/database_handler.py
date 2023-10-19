import sys
from sqlalchemy import text
from helpers.logging import get_logger
from helpers.database_connection import get_engine
from helpers.queries import *


sys.path.insert(0, "/usr/local/airflow/dags")

logger = get_logger()
credentials = {
    "host": "forecast-catalogs-postgres-1",
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
            conn.execute(text(query_schema_stg))
            conn.execute(text(query_schema_dim))
            conn.execute(text("COMMIT;"))
        except Exception as e:
            logger.info(f"Postgres Error: {e}")
        finally:
            conn.close()


def create_tables():
    engine = get_engine(**credentials)
    with engine.begin() as conn:
        try:
            logger.info("Creating staging tables.")
            conn.execute(text(query_avg_table))
            conn.execute(text(query_stg_table))
            logger.info("Creating dimension tables.")
            conn.execute(text(query_dim_table))
            conn.execute(text(query_dim_hist_table))
            conn.execute(text("COMMIT;"))
        except Exception as e:
            logger.info(f"Postgres Error: {e}")
        finally:
            conn.close()


def load_dwh():
    engine = get_engine(**credentials)
    with engine.begin() as conn:
        try:
            logger.info("Updating historical table.")
            conn.execute(text(update_dim_hist))
            logger.info("Inserting historical table.")
            conn.execute(text(insert_dim_hist))
            logger.info("Deleting historical table.")
            conn.execute(text(delete_dim))
            logger.info("Inserting dim table.")
            conn.execute(text(insert_dim))
            logger.info("Truncating staging tables.")
            conn.execute(text(truncate_stg))
            conn.execute(text(truncate_stg_avg))
            conn.execute(text("COMMIT;"))
        except Exception as e:
            logger.info(f"Postgres Error: {e}")
        finally:
            conn.close()
