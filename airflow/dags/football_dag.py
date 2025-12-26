from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
        
default_args={
    'owner': 'hadoop-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 2),
    'email_on_failure': False, 
    'email_on_retry': False,
    'retries': 3, 
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2)
}

dag = DAG(
    dag_id='football_weekly_batch_pipeline',
    default_args=default_args,
    description='Weekly football data processing - Bronze to Silver to Gold',
    schedule_interval='0 2 * * 1',
    catchup=False,
    max_active_runs=1,
    tags=['football', 'batch', 'weekly', 'spark'],
)

from ingestion.api.batch import get_historical_football_data, insert_postgres, write_to_bronze_layer
from batch.football_processor import initial_load_task, full_pipeline_task

def is_initial_load():
    return Variable.get("INITIAL_LOAD", default_var="false") == "true"

ingestion_task = PythonOperator(
    task_id='ingest_football_data',
    python_callable=get_historical_football_data,
    dag=dag
)

postgres_task = PythonOperator(
    task_id='insert_football_postgres',
    python_callable=insert_postgres,
    dag=dag
)

hdfs_bronze_task = PythonOperator(
    task_id='write_to_hdfs',
    python_callable=write_to_bronze_layer,
    dag=dag
)

run_initial_load = PythonOperator(
    task_id='run_initial_load',
    python_callable=initial_load_task,
    execution_timeout=timedelta(hours=4),
    dag=dag
)

spark_processing_task = PythonOperator(
    task_id='spark_bronze_to_gold',
    python_callable=full_pipeline_task,
    execution_timeout=timedelta(hours=2),
    dag=dag
)

branch_task = BranchPythonOperator(
    task_id="branch_initial_or_regular",
    python_callable=lambda: (
        "run_initial_load"
        if is_initial_load()
        else "spark_bronze_to_gold"
    ),
    dag=dag
)

end = EmptyOperator(task_id="end", dag=dag)

ingestion_task >> [postgres_task, hdfs_bronze_task]
[postgres_task, hdfs_bronze_task] >> branch_task

branch_task >> run_initial_load >> end
branch_task >> spark_processing_task >> end