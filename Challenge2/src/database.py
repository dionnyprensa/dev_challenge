import time
import pandas as pd
from sqlalchemy import create_engine
import sys

sys.path.append('../../opt/airflow/')
import src.config_env as env


def get_connection_postgres():
    user = env.POSTGRES_USER
    password = env.POSTGRES_PASSWORD
    host = env.POSTGRES_HOST
    db_name = env.POSTGRES_DB_NAME

    url = f'postgresql://{user}:{password}@{host}:5432/{db_name}'

    engine_postgres = create_engine(url)
    conn = engine_postgres.connect().execution_options(autocommit=True)

    return conn


def get_dataframe_postgres(query, conn=None):
    _conn = get_connection_postgres()
    
    _conn = conn if bool(conn) else _conn

    df = pd.read_sql(query, _conn)
    
    return df