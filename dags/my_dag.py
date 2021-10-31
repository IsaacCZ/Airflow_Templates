from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from includes.vs_modules.test import hello


args = {
    'owner': 'Jim',
    'start_date': days_ago(1) # make start date in the past
}

dag = DAG(
    dag_id='first',
    default_args=args,
    schedule_interval='*/1 * * * *' # make this workflow happen every day
)

with dag:
    hello_world = PythonOperator(
        task_id='hello',
        python_callable=hello,
        # provide_context=True
    )