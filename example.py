from datetime import datetime
from airflow.models import DAG
from airflow.operators.bash import BashOperator


default_args = {
'Owner':'Md Asif',
    'depends_on_past': False,
    'email': ['mdasif.uem@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

ingestion_date = datetime.now.strftime("%d%m%Y")
partition_ingestion_date = datetime.now.strftime("%d-%m-%Y")
source = "/home/asif/source_systems/test_files"
destination = "/home/asif/destination/archived"

with DAG('file_archive_pipeline',default_args=default_args,
    start_date = datetime(2022,6,11),
    schedule_interval='@daily',
    catchup=False) as dag:
    file_archive_task = BashOperator(
        task_id = 'file_archive_task',
        bash_command = f"mv {source}/{ingestion_date}*.csv {destination}/{partition_ingestion_date}",
    )

    file_archive_task
