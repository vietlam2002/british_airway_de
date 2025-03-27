from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

schedule_interval = timedelta(days=1)
start_date = datetime(2025, 3, 27)
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id="british_pipeline",
    schedule_interval=schedule_interval,
    default_args=default_args,
    start_date=start_date,
    catchup=True,
    max_active_runs=1
) as dag:
    scrape_british_data = BashOperator(
        task_id="scrape_british_data",
        bash_command="chmod -R 777 /opt/***/data && python /opt/airflow/tasks/scraper_extract/scraper.py"
    )
    note = BashOperator(
        task_id="note",
        bash_command="echo 'Succesfull extract data to raw_data.csv'"
    )
scrape_british_data >> note