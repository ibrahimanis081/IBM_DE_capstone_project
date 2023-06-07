from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'someone',
    'start_date': datetime(2023, 6, 4),
    'email': 'someoneemail@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False
}

dag = DAG(
    'process_web_log',
    default_args=default_args,
    description='DAG for processing web log',
    schedule_interval='@daily'
)

extract_data_task = BashOperator(
    task_id='extract_data',
    bash_command='awk \'{print $1}\' ./accesslog.txt > extracted_data.txt',
    dag=dag
)

transform_data_task = BashOperator(
    task_id='transform_data',
    bash_command='grep -v "198.46.149.143" extracted_data.txt > transformed_data.txt',
    dag=dag
)

load_data_task = BashOperator(
    task_id='load_data',
    bash_command='tar -cf weblog.tar transformed_data.txt',
    dag=dag
)

extract_data_task >> transform_data_task >> load_data_task
