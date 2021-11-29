import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

# instantiating the Postgres Operator

with DAG(
    dag_id="DW",
    start_date=datetime.datetime(2020, 2, 2),
    schedule_interval="@once",
    catchup=False,
) as dag:
    user_purchase_table = PostgresOperator(
        task_id="create_table_dw",
        postgres_conn_id="my_redshift",
        sql="""
            CREATE TABLE IF NOT EXISTS user_behavior_metric (
            customerid INTEGER,  
            amount_spent decimal(18, 5),
            review_score INTEGER,
            review_count INTEGER,  
            insert_date DATE;
    
          """,
        
       )