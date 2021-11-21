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
            Drop Table user_purchase
    
          """,
        
       )
