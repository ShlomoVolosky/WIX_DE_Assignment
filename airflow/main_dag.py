from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'finance_pipeline',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'finance_pipeline_dag',
    default_args=default_args,
    description='Fetch from Polygon + Frankfurter and load to DW',
    schedule_interval='@daily',  # or as needed
    catchup=False
) as dag:

    # Example: run main_pipeline.py from your container or from a local venv
    run_pipeline = BashOperator(
        task_id='run_pipeline',
        bash_command='python /usr/local/airflow/dags/src/pipeline/main_pipeline.py'
    )

