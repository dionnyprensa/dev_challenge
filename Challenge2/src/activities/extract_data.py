import os
import sys
import warnings

import numpy as np

sys.path.append('../../opt/airflow/')
from src.utils import *


def run():
    warnings.filterwarnings("ignore")
    print('extract_data')

    clean_stage_files()

    data_types_deposits = {
        'id': np.int64,
        'event_timestamp': np.datetime64,
        'user_id': np.str_,
        'amount': np.float64,
        'currency': np.str_,
        'tx_status': np.str_
    }
    df_deposits = extract_from_file_to_df(
        path=get_file_from(file='deposit', zone='temp'),
        headers=data_types_deposits.keys(),
        data_types=data_types_deposits
    )

    data_types_events = {
        'id': np.int64,
        'event_timestamp': np.datetime64,
        'user_id': np.str_,
        'event_name': np.str_
    }
    df_events = extract_from_file_to_df(
        path=get_file_from(file='event', zone='temp'),
        headers=data_types_events.keys(),
        data_types=data_types_events
    )

    data_types_users = {
        'user_id': np.str_
    }
    df_users = extract_from_file_to_df(
        path=get_file_from(file='user_id', zone='temp'),
        headers=data_types_users.keys(),
        data_types=data_types_users
    )

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
        path=get_file_from(file='withdrawals', zone='temp'),
        headers=data_types_withdrawals.keys(),
        data_types=data_types_withdrawals
    )

    load_df_to_file(df_deposits, 'stage', 'deposits')
    load_df_to_file(df_events, 'stage', 'events')
    load_df_to_file(df_users, 'stage', 'users')
    load_df_to_file(df_withdrawals, 'stage', 'withdrawals')
