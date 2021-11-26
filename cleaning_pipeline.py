import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

import pandas as pd


path = "/home/juan/Documents/Data_engineering/"

def clean_scooter():
    df = pd.read_csv(path + 'scooter.csv')
    df.drop(columns=['region_id'], inplace=True)
    df.columns = [x.lower() for x in df.columns]
    df['started_at']=pd.to_datetime(df['started_at'],
            format='%m/%d/%Y %H:%M'
        )

    df.to_csv(path + 'clean_scooter.csv', index=False)


def filter_data():
    df = pd.read_csv(path + 'clean_scooter.csv')
    from_date = '2019-05-23'
    to_date = '2019-06-23'

    to_from_df = df.where(
        (df['started_at'] > from_date) & (df['started_at'] < to_date)
    )

    to_from_df.dropna(inplace=True)

    to_from_df.to_csv(path + 'may23-june23.csv', index=False)


default_args = {
    'owner': 'JuanLima',
    'start_date': dt.datetime(2021, 11, 24),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'Clean_Data',
        default_args=default_args,
        schedule_interval=timedelta(minutes=5)
    ) as dag:

    initialize_env = BashOperator(
        task_id='InitializeVirtualEnv',
        bash_command='source ' + path + 'env/bin/activate' 
    )

    clean_data = PythonOperator(
        task_id='clean',
        python_callable=clean_scooter
    )

    select_data = PythonOperator(
        task_id='filter',
        python_callable=filter_data
    )

initialize_env >> clean_data >> select_data


if __name__ == '__main__':
    clean_scooter()
    filter_data()