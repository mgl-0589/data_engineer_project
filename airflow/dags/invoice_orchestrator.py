import sys
import os
from airflow import DAG
from airflow.decorators import task
from airflow.sensors.filesystem import FileSensor
# from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime, timedelta
from dotenv import load_dotenv

# load variables from .env
load_dotenv()

#

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
    'description': 'DAG to orchestrate ETL process of invoice data',
    'start_date': datetime(2025, 1, 1),
    'catchup': False,
}

# Define the DAG
with DAG(
    dag_id='invoices_data_orchestrator',
    default_args=default_args,
    schedule=timedelta(weeks=1),
    tags=['invoices', 'etl', 'dwh'],
) as dag:
    
    # Sensor to wait for files
    wait_for_files = FileSensor(
        task_id='wait_for_invoices_task',
        filepath='/opt/airflow/data/target/invoices/',
        fs_conn_id='fs_default',
        poke_interval=60,
        timeout=600,
        mode='poke',
    )

    # BashOperator to count files
    count_files = BashOperator(
        task_id="count_invoice_task",
        bash_command='YEAR=$(echo "{{ ds_nodash }}" | cut -c1-4) && ls -l /opt/airflow/data/target/invoices/$YEAR/* | wc -l',
    )

    # TaskFlow API for loading data
    @task(task_id='etl_invoice_data_task')
    def etl_invoice_data(**context) -> None:
        """
        Load invoice data using the main function from invoice_pipeline_utils.
        
        Args:
            context: Airflow context containing execution_date and other runtime variables
        """
        from invoice_pipeline_utils import main
        execution_date = context.get('execution_date')
        year = execution_date.year if execution_date else None
        try:
            main(year)
        except Exception as e:
            raise ValueError(f"Failed to process invoice data: {str(e)}") from e
        
    # DockerOperator to run dbt models
    run_dbt_models = DockerOperator(
        task_id='staging_invoice_data_task',
        image='ghcr.io/dbt-labs/dbt-postgres:1.9.latest',
        command='run',
        working_dir='/usr/app',
        mounts=[
            Mount(
                source='/home/mgl/repos/coffee_shop_data_project/dbt/coffee_shop_project',
                target= '/usr/app',
                type= 'bind'),
            Mount(
                source= '/home/mgl/repos/coffee_shop_data_project/dbt/profiles.yml',
                target= '/root/.dbt/profiles.yml',
                type= 'bind'
            )
        ],
        network_mode='coffee_shop_data_project_etl-network',
        docker_url='unix://var/run/docker.sock',
        auto_remove='success',
        environment={
            'AIRFLOW_POSTGRES_HOST': os.getenv('AIRFLOW_POSTGRES_HOST'),
            'AIRFLOW_POSTGRES_USER': os.getenv('AIRFLOW_POSTGRES_USER'),
            'AIRFLOW_POSTGRES_PASSWORD': os.getenv('AIRFLOW_POSTGRES_PASSWORD'),
            'AIRFLOW_POSTGRES_PORT': 5432
        }
    )
        
# Define task dependencies
wait_for_files >> count_files >> etl_invoice_data() >> run_dbt_models


