from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'rafif_data_engineer',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='tech_jobs_kafka_pipeline',
    default_args=default_args,
    start_date=datetime(2026, 10, 20),
    schedule_interval='0 2 * * *', # Jalan otomatis jam 2 pagi
    catchup=False,
    tags=['streaming', 'scrapy', 'kafka']
) as dag:

    # Membangunkan laba-laba dari dalam Docker
    trigger_scraper = BashOperator(
        task_id='run_scrapy_spider',
        bash_command='cd /opt/airflow/scrapper && scrapy crawl jobs_spider'
    )