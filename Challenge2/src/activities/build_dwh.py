import sys
import warnings

import numpy as np

sys.path.append('../../opt/airflow/')
from src.utils import *
from src.database import get_connection_postgres

def clean_stage_tables(conn):
    conn.execute('CALL stage.clean_stage_tables()')

def build_scd_interfaces(conn):
    conn.execute('CALL stage.build_scd_interfaces()')

def build_scd_statuses(conn):
    conn.execute('CALL stage.build_scd_statuses()')

def build_scd_events(conn):
    conn.execute('CALL stage.build_scd_events()')

def build_scd_currencies(conn):
    conn.execute('CALL stage.build_scd_currencies()')

def build_scd_users(conn):
    conn.execute('CALL stage.build_scd_users()')

def load_fact_withdrawals(conn):
    conn.execute('CALL stage.load_fact_withdrawals()')

def load_fact_events(conn):
    conn.execute('CALL stage.load_fact_events()')

def load_fact_deposits(conn):
    conn.execute('CALL stage.load_fact_deposits()')

def clean_stage_tables(conn):
    conn.execute('CALL stage.clean_stage_tables()')


def run():
    warnings.filterwarnings("ignore")
    print('build_dwh')

    conn = get_connection_postgres()

    build_scd_interfaces(conn)

    build_scd_statuses(conn)

    build_scd_events(conn)

    build_scd_currencies(conn)

    build_scd_users(conn)

    load_fact_withdrawals(conn)

    load_fact_events(conn)

    load_fact_deposits(conn)

    clean_stage_tables(conn)