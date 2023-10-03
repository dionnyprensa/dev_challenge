import sys
import os

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

sys.path.append('../../opt/airflow/')
from src.activities.extract_data import run as extractor_runner
from src.activities.transform_data import run as transformer_runner
from src.activities.build_dwh import run as build_dwh

import src.utils as utils


# Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'challenge_2',
    default_args=default_args,
    description='Extract and Load deposit, withdrawal, event and user data historic to DWH',
    schedule_interval='@daily', # Run once a day at midnight
    catchup=False)

# Define the tasks
# extract_data_from_temp = PythonOperator(
#     task_id='extract_data_from_temp',
#     python_callable=extractor_runner,
#     dag=dag,
# )

transform_data_temp_to_stage = PythonOperator(
    task_id='transform_data_temp_to_stage',
    python_callable=transformer_runner,
    dag=dag,
)

process_and_build_data_dwh = PythonOperator(
    task_id='process_and_build_data_dwh',
    python_callable=build_dwh,
    dag=dag,
)


# extract_data_from_temp >> transform_data_temp_to_stage >> process_and_build_data_dwh
transform_data_temp_to_stage >> process_and_build_data_dwh