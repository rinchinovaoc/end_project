import psycopg2
import os
from airflow.hooks.base_hook import BaseHook
from datetime import datetime


conn_id = 'postgre_conn'
path = "F:\\airflow\\dags\\"

def get_conn(conn_id) -> BaseHook.get_connection:
    conn = BaseHook.get_connection(conn_id)
    return conn

def conn_exec_query(conn_id, query):
    pg_conn = get_conn(conn_id)
    pg_hostname, pg_port, pg_username, pg_pass, pg_db = pg_conn.host, pg_conn.port, pg_conn.login, pg_conn.password, pg_conn.schema
    conn = psycopg2.connect(host=pg_hostname, port=pg_port, user = pg_username, password=pg_pass, database = pg_db)
    
    cursor = conn.cursor()
    cursor.execute(query)
    #cursor.execute('SELECT table_name FROM information_schema.tables')
    conn.commit()
    cursor.close()
    conn.close()

def save_input_data(data):
    file_path = path+"data\\"+str(datetime.today())+".txt"
    with open(file_path, "w", encoding="utf8") as file:
        file.write(str(data))
    file.close
    ##conn_exec_query(conn_id = 'postgre_conn', query = "CALL insert_input_data('"+file_path+"');")

