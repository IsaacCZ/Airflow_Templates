import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

# instantiating the Postgres Operator

with DAG(
    dag_id="postgres_operator_dag",
    start_date=datetime.datetime(2020, 2, 2),
    schedule_interval="@once",
    catchup=False,
) as dag:
    user_purchase_table = PostgresOperator(
        task_id="create_user_purchase_table",
        postgres_conn_id="postgres_default",
        sql="""
               CREATE TABLE IF NOT EXISTS user_purchase (
            invoice_number varchar(20),
            stock_code varchar(20),
            detail varchar(1000),
            quantity bigint,
            inovoice_date timestamp,
            unit_price numeric(8,3),
            customer_id varchar(50),
            country varchar(20));
    
          """,
        
       )
