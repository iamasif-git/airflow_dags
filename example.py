from datetime import datetime,timedelta
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
    'tags':['Ingestion']
}

ingestion_date = datetime.now().strftime("%d%m%Y")
partition_ingestion_date = datetime.now().strftime("%d-%m-%Y")
source = "/home/asif/source_systems/test_files"
destination = "/home/asif/destination/archived"

with DAG('file_archive_pipeline',default_args=default_args,
    start_date = datetime(2022,6,1),
    schedule_interval='@daily',
    catchup=False) as dag:
    create_daily_ingestion_folder_task = BashOperator(
        task_id = 'create_daily_ingestion_folder_task',
        bash_command = f"cd {destination} && mkdir {partition_ingestion_date}"

    )
    file_archive_task = BashOperator(
        task_id = 'file_archive_task',
        bash_command = f"mv {source}/test_file_{ingestion_date}*.csv {destination}/{partition_ingestion_date}",
    )

    create_daily_ingestion_folder_task >> file_archive_task 
