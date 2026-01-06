from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "de-team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2)
}

dag = DAG(
    dag_id="weather_monthly_dag_pipeline",
    default_args=default_args,
    description="Monthly weather batch processing - Gold Layer Compute",
    schedule="@monthly",
    catchup=False,
    max_active_runs=1,
    tags=["weather", "monthly", "spark"]
)

from batch.weather_processor import monthly_task

monthly_processing_task = PythonOperator(
    task_id="spark_weather_batch_processing",
    python_callable=monthly_task,
    dag=dag
)

end = EmptyOperator(task_id="end", dag=dag)

monthly_processing_task >> end
