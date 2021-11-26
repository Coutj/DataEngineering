import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import pandas as pd
from sqlalchemy import create_engine
from elasticsearch import Elasticsearch


path = "/home/juan/Documents/Data_engineering/"

def queryPostgresql():
    conn_string = "postgresql://admin:admin@localhost:5000/dataengineering"
    engine =create_engine(conn_string)

    conn = engine.connect()

    query = "SELECT name, city FROM users"
    df = pd.read_sql(query, conn)
    df.to_csv(path + 'postgressqldata.csv')
    print("--- Data Saved ---")


def insertElasticsearch():
    es = Elasticsearch()
    df = pd.read_csv(path + 'postgressqldata.csv')

    for i, r in df.iterrows():
        doc = r.to_json()
        res = es.index(
            index="frompostgressql",
            body=doc
        )

        print(res)


default_args = {
    'owner': 'JuanLima',
    'start_date': dt.datetime(2020, 4, 2),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'MyDBdag',
    default_args=default_args,
    schedule_interval=timedelta(minutes=5)
    ) as dag:

    initialize_env = BashOperator(
        task_id='InitializeVirtualEnv',
        bash_command='source ' + path + 'env/bin/activate' 
    )

    getData = PythonOperator(
        task_id='QueryPostgresSQL',
        python_callable=queryPostgresql
        )

    insertData = PythonOperator(
        task_id='InsertDataElasticsearch',
        python_callable=insertElasticsearch
    )

initialize_env >> getData >> insertData

if __name__ == '__main__':
    queryPostgresql()