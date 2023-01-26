from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


from datetime import datetime

from parse_rss import parse_source

from tasks import conn1

with DAG(dag_id="parse_source", start_date=datetime(2022, 12, 13), schedule="0 0 * * * *", catchup=False, max_active_runs=1) as dag:
    parse_source = PythonOperator(task_id="parse_source", python_callable = parse_source)

    parse_source
