import sys
import os
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

sys.path.append('/opt/airflow/etl')


def safe_main_invoice_callable(**kwargs):
    from invoice_pipeline_utils import main
    execution_date = kwargs.get('execution_date')
    year = execution_date.year if execution_date else None
    return main(year)


default_args = {
    'description': 'DAG to orchestrate ETL process of invoice data',
    'start_date': datetime(2025, 7, 1),
    'catchup': False
}


dag = DAG(
    dag_id='invoices_data_orchestrator',
    default_args=default_args,
    schedule=timedelta(weeks=1)
)


wait_for_files = FileSensor(
    task_id='wait_for_invoices_task',
    filepath='/opt/airflow/data/target/invoices/',
    fs_conn_id='fs_default',
    poke_interval=60,
    timeout=600,
    mode='poke',
    dag=dag
)


count_files = BashOperator(
    task_id='count_invoice_task',
    bash_command='YEAR=$(echo "{{ ds_nodash }}" | cut -c1-4) && ls -l /opt/airflow/data/target/invoices/$(echo "$YEAR")/* | wc -l',
    dag=dag
)


load_data = PythonOperator(
    task_id='load_invoice_data_task',
    python_callable=safe_main_invoice_callable,
    dag=dag
)



wait_for_files >> count_files >> load_data