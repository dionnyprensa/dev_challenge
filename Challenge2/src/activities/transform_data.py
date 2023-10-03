import sys
import warnings

import numpy as np

sys.path.append('../../opt/airflow/')
from src.utils import *
from src.database import get_dataframe_postgres, get_connection_postgres


def process_dim_users(conn):
    data_types_users = {
        'user_id': np.str_
    }

    df_users = extract_from_file_to_df(
        path=get_file_from(file='users', zone='stage'),
        headers=data_types_users.keys(),
        data_types=data_types_users
    )

    df_dim_users = get_dataframe_postgres('SELECT user_id FROM public."DimUsers"' ,conn)

    df_new_users = pd.merge(df_users, df_dim_users, how='left', on='user_id', suffixes=('_l', '_r'))

    if df_new_users.shape[0] > 0:
        result = df_new_users.to_sql(
            name='DimUsers',
            con=conn,
            schema='stage',
            if_exists='replace',
        )

        print('process_dim_users', result)


def process_dim_statuses(conn, df, if_exists='replace'):
    df_dim_statuses = get_dataframe_postgres(
        'SELECT status_name FROM public."DimStatuses"', conn
    )

    df_new_statuses = pd.merge(df, df_dim_statuses, how='left', left_on='tx_status', right_on='status_name')

    if df_new_statuses.shape[0] > 0:
        df_new_statuses = df_new_statuses.drop(['status_name'], axis = 1)

        result = df_new_statuses.to_sql(
            name='DimStatuses',
            con=conn,
            schema='stage',
            if_exists=if_exists
        )

        print('process_dim_statuses', result)


def process_dim_currencies(conn, df, if_exists='replace'):
    df_dim_currencies = get_dataframe_postgres(
        'SELECT currency_name FROM public."DimCurrencies"', conn
    )

    df_new_currencies = pd.merge(df, df_dim_currencies, how='left', left_on='currency', right_on='currency_name')

    if df_new_currencies.shape[0] > 0:
        df_new_currencies = df_new_currencies.drop(['currency_name'], axis = 1)

        result = df_new_currencies.to_sql(
            name='DimCurrencies',
            con=conn,
            schema='stage',
            if_exists=if_exists
        )

        print('process_dim_currencies', result)


def process_dim_interfaces(conn, df, if_exists='replace'):
    df_dim_interfaces = get_dataframe_postgres(
        'SELECT interface_name FROM public."DimInterfaces"', conn
    )

    df_new_interfaces = pd.merge(df, df_dim_interfaces, how='left', left_on='interface', right_on='interface_name')

    if df_new_interfaces.shape[0] > 0:
        df_new_interfaces = df_new_interfaces.drop(['interface_name'], axis = 1)

        result = df_new_interfaces.to_sql(
            name='DimInterfaces',
            con=conn,
            schema='stage',
            if_exists=if_exists
        )

        print('process_dim_interfaces', result)


def process_dim_events(conn, df, if_exists='replace'):
    df_dim_events = get_dataframe_postgres(
        'SELECT event_name FROM public."DimEvents"', conn
    )

    df_new_events = pd.merge(df, df_dim_events, how='left', on='event_name')

    if df_new_events.shape[0] > 0:
        result = df_new_events.to_sql(
            name='DimEvents',
            con=conn,
            schema='stage',
            if_exists=if_exists
        )

        print('process_dim_events', result)


def process_fact_withdrawals(conn):
    data_types_withdrawals = {
        'id': np.int64,
        'event_timestamp': np.datetime64,
        'user_id': np.str_,
        'amount': np.float64,
        'interface': np.str_,
        'currency': np.str_,
        'tx_status': np.str_
    }
    df_withdrawals = extract_from_file_to_df(
        path=get_file_from(file='withdrawals', zone='stage'),
        headers=data_types_withdrawals.keys(),
        data_types=data_types_withdrawals
    )

    if df_withdrawals.shape[0] > 0:
        df_dim_statuses = df_withdrawals['tx_status'].drop_duplicates().to_frame()
        df_dim_interfaces = df_withdrawals['interface'].drop_duplicates().to_frame()
        df_dim_currencies = df_withdrawals['currency'].drop_duplicates().to_frame()

        process_dim_statuses(conn, df_dim_statuses, 'append')
        process_dim_interfaces(conn, df_dim_interfaces, 'append')
        process_dim_currencies(conn, df_dim_currencies, 'append')

        result = df_withdrawals.to_sql(
            name='FactWithdrawals',
            con=conn,
            schema='stage',
            if_exists='replace'
        )

        print('process_fact_withdrawals', result)


def process_fact_events(conn):
    data_types_events = {
        'id': np.int64,
        'event_timestamp': np.datetime64,
        'user_id': np.str_,
        'event_name': np.str_
    }
    df_events = extract_from_file_to_df(
        path=get_file_from(file='events', zone='stage'),
        headers=data_types_events.keys(),
        data_types=data_types_events
    )

    if df_events.shape[0] > 0:
        df_dim_events = df_events['event_name'].drop_duplicates().to_frame()

        process_dim_events(conn, df_dim_events, 'append')

        result = df_events.to_sql(
            name='FactEvents',
            con=conn,
            schema='stage',
            if_exists='replace'
        )

        print('process_fact_events', result)


def process_fact_deposits(conn):
    data_types_deposits = {
        'id': np.int64,
        'event_timestamp': np.datetime64,
        'user_id': np.str_,
        'amount': np.float64,
        'currency': np.str_,
        'tx_status': np.str_
    }
    df_deposits = extract_from_file_to_df(
        path=get_file_from(file='deposits', zone='stage'),
        headers=data_types_deposits.keys(),
        data_types=data_types_deposits
    )

    if df_deposits.shape[0] > 0:
        df_dim_statuses = df_deposits['tx_status'].drop_duplicates().to_frame()
        df_dim_currencies = df_deposits['currency'].drop_duplicates().to_frame()

        process_dim_statuses(conn, df_dim_statuses, 'append')
        process_dim_currencies(conn, df_dim_currencies, 'append')

        result = df_deposits.to_sql(
            name='FactDeposits',
            con=conn,
            schema='stage',
            if_exists='replace'
        )

        print('process_fact_deposits', result)


def run():
    warnings.filterwarnings('ignore')
    print('transform_data')

    conn = get_connection_postgres()
    
    process_dim_users(conn)

    process_fact_withdrawals(conn)

    process_fact_events(conn)

    process_fact_deposits(conn)

