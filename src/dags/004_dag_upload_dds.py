from airflow import DAG
from datetime import timedelta
import json
import pendulum
import os
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup

import logging

pg_dwh = 'PG_WAREHOUSE_CONNECTION'

task_logger = logging.getLogger('airflow.task')


# tz params
local_tz = pendulum.timezone("Europe/Moscow")
local_ds = '{{ dag.timezone.convert(execution_date).strftime("%Y-%m-%d") }}'

dwh_hook = PostgresHook(pg_dwh)


def h_products(date, pg_schema_stg, pg_table_stg, pg_schema_dds, pg_table_dds):

    conn = dwh_hook.get_conn()
    cursor = conn.cursor()   
    # load new data
    sql = f'''
        SELECT object_value::JSON->>'menu' 
        FROM {pg_schema_stg}.{pg_table_stg}
        WHERE update_ts::date = '{date}'
        '''
    cursor.execute(sql)
    # get data to variable
    products = cursor.fetchall()

    task_logger.info(f"today updated {len(products)} restaurants")

    # query to insert
    sql = f'''
        INSERT INTO {pg_schema_dds}.{pg_table_dds} (product_id_bk) 
        VALUES(%s)
        ON CONFLICT (product_id_bk) DO NOTHING
        '''


    if len(products) > 0:

        for i in range(len(products)):
            product_json = json.loads(products[i][0])
            for j in range(len(product_json)):
                cursor.execute(sql, [f"{product_json[j]['_id']}"])
        conn.commit()
        cursor.close() 
        conn.close()  
    else:
        task_logger.info(f"no data for update")

def order_items(date, pg_schema_stg, pg_table_stg):

    conn = dwh_hook.get_conn()
    cursor = conn.cursor()   

    # load order_items data
    sql = f'''
    SELECT object_id, object_value::JSON->>'order_items'
    FROM {pg_schema_stg}.{pg_table_stg}
    WHERE (object_value::JSON->>'date')::date = '{date}'
        '''

    cursor.execute(sql)
    order_items = cursor.fetchall()
    task_logger.info(f"upload information about {len(order_items)} orders")

    # create temp table for order_items
    sql = f'''
            create table if not exists temp_order_items(
                order_id varchar,
                product_id varchar,
                quantity int
        );
        '''
    cursor.execute(sql)

    # insert values in temp table
    sql = f"INSERT INTO temp_order_items (order_id, product_id, quantity) VALUES(%s, %s, %s)"

    
    for i in range(len(order_items)):
        order_id = order_items[i][0]
        products_json = json.loads(order_items[i][1])
        for j in range(len(products_json)):
            cursor.execute(sql, 
                    (f"{order_id}", f"{products_json[j]['id']}", f"{products_json[j]['quantity']}"))

    conn.commit()
    cursor.close() 
    conn.close()

def order_bonuses(date, pg_schema_stg, pg_table_stg):

    conn = dwh_hook.get_conn()
    cursor = conn.cursor()   

    # load order_items data
    sql = f'''
    SELECT event_value::JSON->>'order_id', event_value::JSON->>'product_payments'
    FROM {pg_schema_stg}.{pg_table_stg}
    WHERE event_type = 'bonus_transaction' AND (event_value::JSON->>'order_date')::date = '{date}'
        '''
    cursor.execute(sql)
    order_bonuses = cursor.fetchall()
    task_logger.info(f"upload information about {len(order_bonuses)} orders")

    # create temp table for order_items
    sql = f'''
            create table if not exists temp_order_bonuses(
                order_id varchar,
                product_id varchar,
                bonus_payment numeric(8, 2),
                bonus_grant numeric(8, 2)
        );
        '''
    cursor.execute(sql)

    # insert values in temp table
    sql = f"INSERT INTO temp_order_bonuses (order_id, product_id, bonus_payment, bonus_grant) VALUES(%s, %s, %s, %s)"

    
    for i in range(len(order_bonuses)):
        order_id = order_bonuses[i][0]
        products_json = json.loads(order_bonuses[i][1])
        for j in range(len(products_json)):
            cursor.execute(sql, 
                    (f"{order_id}", f"{products_json[j]['product_id']}", f"{products_json[j]['bonus_payment']}", f"{products_json[j]['bonus_grant']}"))

    conn.commit()
    cursor.close() 
    conn.close()   

def get_products(date, pg_schema_stg, pg_table_stg):

    conn = dwh_hook.get_conn()
    cursor = conn.cursor()   
    # load new data
    sql = f'''
        SELECT update_ts, object_value::JSON->>'menu'
        FROM {pg_schema_stg}.{pg_table_stg}
        WHERE update_ts::date = '{date}'
        '''
    cursor.execute(sql)
    # get data to variable
    products = cursor.fetchall()

    task_logger.info(f"today updated {len(products)} restaurants for products")

     # create temp table for products
    sql = f'''
            create table if not exists temp_products(
                product_id varchar,
                product_name varchar,
                product_price numeric(8,2),
                product_category varchar,
                update_ts timestamp
        );
        '''
    cursor.execute(sql)

    # insert values in temp table
    sql = f"INSERT INTO temp_products (product_id, product_name, product_price, product_category, update_ts) VALUES(%s, %s, %s, %s, %s)"

    
    for i in range(len(products)):
        update_ts = products[i][0]
        products_json = json.loads(products[i][1])
        for j in range(len(products_json)):
            cursor.execute(sql, 
                (f"{products_json[j]['_id']}", f"{products_json[j]['name']}", f"{products_json[j]['price']}", f"{products_json[j]['category']}", f"{update_ts}")
            )

    conn.commit()
    cursor.close() 
    conn.close()


# params for dag
default_args = {
    'owner':'maks',
    'retries':1,
    'retry_delay': timedelta (minutes = 60)
}


dag = DAG('04_dag_upload_dds',
        start_date=pendulum.datetime(2022, 8, 27, tz=local_tz),
        catchup=True,
        schedule_interval='30 0 * * *',
        max_active_runs=1,
        default_args=default_args)

with TaskGroup(group_id = 'update_hubs', dag=dag) as update_hubs:

    h_products = PythonOperator(
        task_id = 'h_products',
        python_callable = h_products,
        op_kwargs = {
            'date' : local_ds,
            'pg_schema_dds' : 'prod_dv_dds',
            'pg_table_dds' : 'h_products',
            'pg_schema_stg' : 'prod_dv_stg',
            'pg_table_stg' : 'order_system_restaurants'
        },
        dag=dag
    )

    h_upd = [
        PostgresOperator(
            task_id = f"{task[3:-4]}",
            postgres_conn_id = pg_dwh,
            sql = f"sql/dv/hubs/{task}",
            dag = dag
    ) for task in os.listdir('/lessons/dags/sql/dv/hubs')
    ]

with TaskGroup(group_id = 'update_links', dag=dag) as update_links:
    order_items = PythonOperator(
                task_id = 'order_items',
                python_callable = order_items,
                op_kwargs = {
                    'date' : local_ds,
                    'pg_schema_stg' : 'prod_dv_stg',
                    'pg_table_stg' : 'order_system_orders',
                },
                dag = dag
    )

    order_bonuses = PythonOperator(
                task_id = 'order_bonuses',
                python_callable = order_bonuses,
                op_kwargs = {
                    'date' : local_ds,
                    'pg_schema_stg' : 'prod_dv_stg',
                    'pg_table_stg' : 'bonus_system_outbox',
                },
                dag = dag
    )

    l_orders = PostgresOperator(
                task_id = 'l_orders',
                postgres_conn_id = pg_dwh,
                sql = 'sql/dv/links/01_l_orders.sql',
                dag = dag
    )

    clear_temp = PostgresOperator(
                task_id = 'clear_temp',
                postgres_conn_id = pg_dwh,
                sql = 'sql/dv/links/02_clear_temp_tables.sql',
                dag = dag
    )
    [order_items, order_bonuses] >> l_orders >> clear_temp

with TaskGroup(group_id = 'update_satellites', dag=dag) as update_satellites:

    with TaskGroup(group_id = 's_upd', dag=dag) as s_upd:

        s_update = [
            PostgresOperator(
                task_id = f"{task[3:-4]}",
                postgres_conn_id = pg_dwh,
                sql = f"sql/dv/satellites/{task}",
                dag = dag
        ) for task in os.listdir('/lessons/dags/sql/dv/satellites') if task != 's_products']  

        s_update


    with TaskGroup(group_id = 's_products', dag=dag) as s_products:

        get_products = PythonOperator(
                    task_id = 'get_products',
                    python_callable = get_products,
                    op_kwargs = {
                        'date' : local_ds,
                        'pg_schema_stg' : 'prod_dv_stg',
                        'pg_table_stg' : 'order_system_restaurants',
                    },
                    dag = dag
        )

        upd_products = PostgresOperator(
                task_id = f"upd_products",
                postgres_conn_id = pg_dwh,
                sql = f"sql/dv/satellites/s_products/01_s_products.sql",
                dag = dag
            )

        clear_temp = PostgresOperator(
                task_id = f"clear_temp",
                postgres_conn_id = pg_dwh,
                sql = f"sql/dv/satellites/s_products/02_clear_temp_table.sql",
                dag = dag
            )
        get_products >> upd_products >> clear_temp

    [s_upd, s_products]

update_hubs >> [update_links, update_satellites]
