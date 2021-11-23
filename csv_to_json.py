import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pandas as pd

def csv_to_json():
    df = pd.read_csv('/home/juan/Documents/Data_engineering/myCSV.CSV')

    for i, r in df.iterrows():
        print(r['name'])

    df.to_json('/home/juan/Documents/Data_engineering/fromAirflow.json', orient='records')

default_args = {
    'owner': 'juanlima',
    'start_date': dt.datetime(2021, 11, 9),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5)
}

with DAG (
        'MyCSVDAG',
        default_args=default_args,
        schedule_interval=timedelta(minutes=5),
        # '0 * * * *',
    ) as dag:

    print_starting = BashOperator(
        task_id='starting',
        bash_command='echo "I am reading the CSV now..."'
    )

    csvJson = PythonOperator (
        task_id='convertCSVtoJson',
        python_callable=csv_to_json
    )

# print_starting.set_downstream(csvJson)
# csvJson.set_upstream(print_starting)

print_starting >> csvJson

if __name__ == '__main__':
    csv_to_json()