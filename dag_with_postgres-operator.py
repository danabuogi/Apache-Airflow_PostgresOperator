from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

import pendulum

sydney_timezone = pendulum._safe_timezone("Australia/Sydney")

default_args = {"owner":"Dan", "retries": 5, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="dag_with_postgres_operator_v04", 
    default_args = default_args, 
    start_date=datetime(2023,2,14, tzinfo = sydney_timezone), 
    schedule_interval="0 14 * * Mon-Fri"
    ) as dag:
    task1 = PostgresOperator(task_id = "create_customers_tbl", 
                             postgres_conn_id="postgres_localhost",
                             sql = """ CREATE TABLE if not exists customers_tbl
                             (date date,
                             customer_name varchar,
                             customer_id varchar,
                             primary key (customer_id))""")
    task2 = PostgresOperator(task_id = "Insert_into_customers_tbl",
                             postgres_conn_id= "postgres_localhost",
                             sql = """INSERT INTO customers_tbl (date, customer_name, customer_id)
                             values ('{{ds}}', 'Julius Moore', 'cu3-300');""")
    task1 >> task2

