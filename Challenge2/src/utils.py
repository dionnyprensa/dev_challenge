import sys
import os
from datetime import datetime

import numpy as np
import pandas as pd

sys.path.append('../../opt/airflow/')
from src.constants import FLOAT_FORMAT, DATE_FORMAT

DATA_LAKE_ROOT_PATH = os.path.join(os.getcwd(), 'datalake')
DATA_LAKE_TEMP = os.path.join(DATA_LAKE_ROOT_PATH, 'temp')
DATA_LAKE_STAGE = os.path.join(DATA_LAKE_ROOT_PATH, 'stage')

DEPOSITS_TEMP_PATH = os.path.join(DATA_LAKE_TEMP, 'deposit_data.csv')
EVENTS_TEMP_PATH = os.path.join(DATA_LAKE_TEMP, 'event_data.csv')
USERS_TEMP_PATH = os.path.join(DATA_LAKE_TEMP, 'user_id_data.csv')
WITHDRAWALS_TEMP_PATH = os.path.join(DATA_LAKE_TEMP, 'withdrawals_data.csv')


def get_path_by_zone(zone='stage'):
    print('get_path_by_zone')
    source = DATA_LAKE_STAGE if zone == 'stage' else DATA_LAKE_TEMP

    return source


def get_file_from(file, zone='stage'):
    print('get_file_from')
    source = get_path_by_zone(zone=zone)
    sufix = '' if zone == 'stage' else '_sample_data'

    return os.path.join(source, f'{file}{sufix}.csv')


def extract_from_file_to_df(path, headers, data_types={}):
    print('extract_from_file_to_df')
    options = {
        'filepath_or_buffer': path,
        'usecols': headers,
        'dtype': data_types
    }

    if headers and isinstance(headers, list) and len(headers) > 1:
        options.pop("usecols")

    if data_types and isinstance(data_types, dict) and len(data_types) > 1:
        options.pop("dtype")

    return pd.read_csv(**options)


def load_df_to_file(df, to_, filename):
    print('load_df_to_file', filename)
    zone = get_path_by_zone(zone=to_)

    path_to_filename = os.path.join(zone, filename + '.csv')

    if os.path.exists(path_to_filename):
        os.remove(path_to_filename)

    df.to_csv(
        path_to_filename,
        float_format=FLOAT_FORMAT,
        date_format=DATE_FORMAT,
        index=False
    )

def clean_stage_files():
    print('clean_stage_files')
    directory_path = get_path_by_zone(zone='stage')

    for filename in os.listdir(directory_path):
        file_path = os.path.join(directory_path, filename)
        
        if os.path.isfile(file_path):
            os.remove(file_path)