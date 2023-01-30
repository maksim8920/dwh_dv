import requests
import json
from psycopg2.extras import execute_values
import pendulum
from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

task_logger = logging.getLogger('airflow.task')

# tz params
local_tz = pendulum.timezone("Europe/Moscow")
local_ds = '{{ dag.timezone.convert(execution_date).strftime("%Y-%m-%d") }}'
prev_local_ds = '{{ dag.timezone.convert(yesterday_ds).strftime("%Y-%m-%d") }}'

# connections
api_conn = BaseHook.get_connection('http_conn_id')
postgres_conn = 'PG_WAREHOUSE_CONNECTION'

# params for API
nickname = 'mpopov'
api_key = json.loads(api_conn.extra)['api_key']
base_url = api_conn.host

params = {"X-Nickname" : nickname,
         'X-API-KEY' : api_key,
         }

method_couriers = '/couriers'
method_deliveries = '/deliveries'

# connect to dwh
dwh_hook = PostgresHook(postgres_conn)



def upload_couriers(pg_schema, pg_table_dwh):

    conn = dwh_hook.get_conn()
    cursor = conn.cursor()

    # idempotency
    dwh_hook.run(sql = f"TRUNCATE {pg_schema}.{pg_table_dwh}")

    rep = requests.get(f'https://{base_url}{method_couriers}',headers = params).json()

    columns = ','.join([i for i in rep[0]])
    values = [[value for value in rep[i].values()] for i in range(len(rep))]

    sql = f"INSERT INTO {pg_schema}.{pg_table_dwh} ({columns}) VALUES %s"
    execute_values(cursor, sql, values)
    
    conn.commit()
    cursor.close()
    conn.close()

# get data deliveries
def upload_deliveries(prev_date, today_date, pg_schema, pg_table_dwh):

    conn = dwh_hook.get_conn()
    cursor = conn.cursor()

    # time interval
    start = f"{prev_date} 00:00:00"
    end = f"{today_date} 23:59:59"

    # idempotency
    dwh_hook.run(sql = f"DELETE FROM {pg_schema}.{pg_table_dwh} WHERE order_ts::date BETWEEN '{prev_date}' AND '{today_date}'")

    # get data
    offset = 0
    while True:    
        deliver_rep = requests.get(f'https://{base_url}{method_deliveries}/?sort_field=order_ts&sort_direction=asc&from={start}&to={end}&offset={offset}',
                            headers = params).json()

        # check data
        if len(deliver_rep) == 0:
            conn.commit()
            cursor.close()
            conn.close()
            task_logger.info(f'Writting {offset} rows')
            break

        # writting to dwh 
        columns = ','.join([i for i in deliver_rep[0]])
        values = [[value for value in deliver_rep[i].values()] for i in range(len(deliver_rep))]

        sql = f"INSERT INTO {pg_schema}.{pg_table_dwh} ({columns}) VALUES %s"
        execute_values(cursor, sql, values)

        offset += len(deliver_rep)  
                  
# params for dag
default_args = {
    'owner':'maks',
    'retries':1,
    'retry_delay': timedelta (seconds = 60)
}


dag = DAG('03_dag_upload_stage_from_api',
        start_date=pendulum.datetime(2022, 8, 27, tz=local_tz),
        catchup=True,
        schedule_interval='5 0 * * *',
        max_active_runs=1,
        default_args=default_args)


upload_deliveries = PythonOperator(
            task_id = 'api_deliveries',
            python_callable = upload_deliveries,
            op_kwargs = {
                'prev_date' : prev_local_ds,
                'today_date' : local_ds,
                'pg_schema' : 'prod_dv_stg',
                'pg_table_dwh' : 'couriers_system_deliveries',
            },
            dag = dag
)


upload_couriers = PythonOperator(
    task_id = 'upload_couriers',
    python_callable = upload_couriers,
    op_kwargs = {
                'start_date' : local_ds,
                'pg_schema' : 'prod_dv_stg',
                'pg_table_dwh' : 'couriers_system_couriers',
            },
    dag = dag
)
[upload_deliveries, upload_couriers]


