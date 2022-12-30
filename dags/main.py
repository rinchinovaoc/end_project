from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


from datetime import datetime

from parse_rss import parse_source

from tasks import conn1


#with DAG(dag_id="first_dag", start_date=datetime(2022, 12, 13), schedule="40-45 2 * * *", catchup=False, max_active_runs=1) as dag:
with DAG(dag_id="aaa", start_date=datetime(2022, 12, 13), schedule="0 0 * * * *", catchup=False, max_active_runs=1) as dag:
    task1 = PythonOperator(task_id="conn1", python_callable = conn1)

    task1


with DAG(dag_id="parse_source", start_date=datetime(2022, 12, 13), schedule="0 0 * * * *", catchup=False, max_active_runs=1) as dag:
    parse_source = PythonOperator(task_id="parse_source", python_callable = parse_source)

    parse_source
