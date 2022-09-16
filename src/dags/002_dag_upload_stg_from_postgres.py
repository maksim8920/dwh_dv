
import logging
import pendulum
from airflow import DAG
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


task_logger = logging.getLogger('airflow.task')

# tz params
local_tz = pendulum.timezone("Europe/Moscow")
local_ds = '{{ dag.timezone.convert(execution_date).strftime("%Y-%m-%d") }}'

# connections
pg_dwh = 'PG_WAREHOUSE_CONNECTION'
pg_source = 'PG_ORIGIN_BONUS_SYSTEM_CONNECTION'

# hook for source
source_hook = PostgresHook(pg_source)
# hook for dwh
dwh_hook = PostgresHook(pg_dwh)
engine_dwh = dwh_hook.get_sqlalchemy_engine()

def load_outbox_to_stage(pg_schema_source, pg_table_source, pg_schema_dwh, pg_table_dwh, date):

    # idempotency
    dwh_hook.run(sql = f"DELETE FROM {pg_schema_dwh}.{pg_table_dwh} WHERE event_ts::date = '{date}'")

    # get data
    df = source_hook.get_pandas_df(sql = f"select event_type, event_value, event_ts from {pg_schema_source}.{pg_table_source} WHERE event_ts::date = '{date}'")
    task_logger.info(f"{len(df)} rows upload from {pg_table_source}")

    # load data
    df.to_sql(pg_table_dwh, engine_dwh, schema=pg_schema_dwh, if_exists='append', index=False)
    task_logger.info('data load succesfull')

def load_users_to_stage(pg_schema_source, pg_table_source, pg_schema_dwh, pg_table_dwh):

    # idempotency
    dwh_hook.run(sql = f"TRUNCATE {pg_schema_dwh}.{pg_table_dwh}")

    # get data
    df = source_hook.get_pandas_df(sql = f"select * from {pg_schema_source}.{pg_table_source}")
    task_logger.info(f"{len(df)} rows upload from {pg_table_source}")

    df = df.rename(columns = {'id' : 'bonus_user_id'})

    # load data
    df.to_sql(pg_table_dwh, engine_dwh, schema=pg_schema_dwh, if_exists='append', index=False)
    task_logger.info('data load succesfull')


# params for dag
default_args = {
    'owner':'maks',
    'retries':1,
    'retry_delay': timedelta (seconds = 60)
}


dag = DAG('02_dag_upload_stage_from_pg',
        start_date=pendulum.datetime(2022, 8, 27, tz=local_tz),
        catchup=True,
        schedule_interval='5 0 * * *',
        max_active_runs=1,
        default_args=default_args)

pg_outbox = PythonOperator(
            task_id = 'pg_outbox',
            python_callable = load_outbox_to_stage,
            op_kwargs = {
                'pg_schema_source' : 'public',
                'pg_table_source' : 'outbox',
                'pg_schema_dwh' : 'prod_dv_stg',
                'pg_table_dwh' : 'bonus_system_outbox',
                'date' : local_ds
            },
            dag = dag
)

pg_users = PythonOperator(
            task_id = 'pg_users',
            python_callable = load_users_to_stage,
            op_kwargs = {
                'pg_schema_source' : 'public',
                'pg_table_source' : 'users',
                'pg_schema_dwh' : 'prod_dv_stg',
                'pg_table_dwh' : 'bonus_system_users'
            },
            dag = dag
)

[pg_outbox, pg_users]