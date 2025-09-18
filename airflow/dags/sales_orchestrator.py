import sys
import os
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
# from airflow.operators.email import EmailOpertor
from datetime import datetime, timedelta


sys.path.append('/opt/airflow/etl')



def safe_main_sales_callable():
    from sales_pipeline_utils import main
    return main()


default_args = {
    'description': 'DAG to orchestrate ETL process of sales and sales data',
    'start_date': datetime(2025, 7, 1),
    'catchup': False
}


dag = DAG(
    dag_id='sales_data_orchestrator',
    default_args=default_args,
    schedule=timedelta(weeks=1)
)


wait_for_files = FileSensor(
    task_id='wait_for_sales_task',
    filepath='/opt/airflow/data/invoices/',
    fs_conn_id='fs_default',
    poke_interval=60,
    timeout=600,
    mode='poke',
    dag=dag
)


count_files = BashOperator(
    task_id='count_sales_task',
    bash_command='ls -l /opt/airflow/data/sales/* | wc -l',
    dag=dag
)


extract_data = PythonOperator(
    task_id="extract_sales_data_task",
    python_callable=safe_main_sales_callable,
    dag=dag
)


move_files = BashOperator(
    task_id='move_xlsx_files_task',
    bash_command='mv /opt/airflow/data/sales/* /opt/airflow/data/tmp/',
    dag=dag
)


wait_for_files >> count_files >> extract_data >> move_files