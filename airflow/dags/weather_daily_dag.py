from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

defauly_args = {
    "owner": "de-team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_retry": False,
    "email_on_failure": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2)
}

dag = DAG(
    dag_id="weather_daily_batch_pipeline",
    default_args=defauly_args,
    description="Daily weather batch processing - Gold Layer Compute",
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["weather", "daily", "spark"]
)

from batch.weather_processor import daily_task

daily_processing_task = PythonOperator(
    task_id="spark_weather_batch_processing",
    python_callable=daily_task,
    dag=dag
)

end = EmptyOperator(task_id="end", dag=dag)

daily_processing_task >> end