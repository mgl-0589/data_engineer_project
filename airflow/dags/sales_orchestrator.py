import sys
import os
from airflow import DAG
from airflow.decorators import task
from airflow.sensors.filesystem import FileSensor
# from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


# Add ETL path to sys.path for module imports
sys.path.append('/opt/airflow/etl')

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'description': 'DAG to orchestrate ETL process of sales data',
    'start_date': datetime(2025, 1, 1),
    'catchup': False,
}

# Define the DAG
with DAG(
    dag_id='sales_data_orchestrator',
    default_args=default_args,
    schedule='0 22 15 * *',
    tags=['sales', 'etl'],
) as dag:
    
    # Sensor to wait for files
    wait_for_files = FileSensor(
        task_id='wait_for_sales_task',
        filepath='/opt/airflow/data/target/sales/',
        fs_conn_id='fs_default',
        poke_interval=60,
        timeout=600,
        mode='poke',
    )

    # BashOperator to count files
    count_files = BashOperator(
        task_id='count_sales_task',
        bash_command='ls -l /opt/airflow/data/target/sales/* | wc -l',
    )

    # Taskflow API for loading data
    @task(task_id='extract_sales_data_task')
    def extract_data() -> None:
        """
        Extract sales data using the main function from sales_pipeline_utils

        """
        from sales_pipeline_utils import main
        try:
            main()
        except Exception as e:
            raise ValueError(f"Failed to process sales data {str(e)}") from e

# Define task dependencies
wait_for_files >> count_files >> extract_data()