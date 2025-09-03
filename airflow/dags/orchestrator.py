import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta



sys.path.append('/opt/airflow/etl')

def safe_main_callable():
    from pipeline_utils import main
    return main()


default_args = {
    'description': 'DAG to orchestrate ETL process of invoice data',
    'start_date': datetime(2025, 7, 1),
    'catchup': False
}


dag = DAG(
    dag_id='coffee_shop_orchestrator',
    default_args=default_args,
    schedule=timedelta(weeks=1)
)

precheck = BashOperator(
    task_id='data_check_task',
    bash_command='ls -l /opt/airflow/target_data',
    dag=dag
)

# load_data = PythonOperator(
#     task_id='load_data_task',
#     python_callable=safe_main_callable,
#     dag=dag
# )

# with dag:
#     task2 = PythonOperator(
#         task_id='load_data_task',
#         python_callable=safe_main_callable
#     ),
#     task2 = BashOperator(
#         task_id='check_for_files',
#         bash_command='ls -l /opt/airflow/source_data'
#     )


# precheck >> load_data