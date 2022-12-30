from parse_rss import parse_source

from work_psql import conn_exec_query

def conn1():
    conn_exec_query(conn_id = 'postgre_conn', query = 'SELECT table_name FROM information_schema.tables')
    #conn_exec_query()

