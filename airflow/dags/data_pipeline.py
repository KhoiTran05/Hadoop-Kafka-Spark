# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.operators.bash import BashOperator
# from airflow.operators.empty import EmptyOperator
# from airflow.utils.dates import days_ago
        
# #Dag default arguments
# default_args={
#     'owner': 'hadoop-team',
#     'depends_on_past': False, # Kiểm tra Task tương ứng trong LẦN CHẠY TRƯỚC ĐÓ
#     'start_date': days_ago(1),
#     'email_on_failure': False, 
#     'email_on_retry': False,
#     'retries': 3, # 1 Task
#     'retry_delay': timedelta(minutes=5),
#     'execution_timeout': timedelta(hours=2)
# }

# #Create dag
# dag = DAG(
#     dag_id='comprehensive_data_pipeline',
#     default_args=default_args,
#     description='Complete data pipeline with Hadoop',
#     schedule_interval=timedelta(hours=6),
#     catchup=False,
#     max_active_runs=1,
#     tags = ['hadoop', 'hive', 'spark']
# )

# from ingestion.api.streaming import fetch_weather_data, fetch_stock_data, validate_data

# #Task definitions
# start_task = EmptyOperator(
#     task_id='start_pipeline',
#     dag=dag
# )

# ingest_weather_task = PythonOperator(
#     task_id='ingest_weather_data',
#     python_callable=fetch_weather_data,
#     dag=dag
# )

# ingest_stock_task = PythonOperator(
#     task_id='ingest_stock_data',
#     python_callable=fetch_stock_data,
#     dag=dag
# )

# validate_data_task = PythonOperator(
#     task_id='validate_data_quality',
#     python_callable=validate_data,
#     dag=dag
# )

# HDFS_LOADER_SCRIPT = '/opt/airflow/ingestion/scripts/load_to_hdfs.sh'

# upload_to_hdfs_task = BashOperator(
#     task_id='upload_to_hdfs',
#     bash_command=HDFS_LOADER_SCRIPT + ' {{ ds }}',
#     cwd='/opt/airflow/',
#     dag=dag
# )

# HIVE_RAW_DDL_SCRIPT = '/opt/airflow/processing/hive/ddl/raw_db.hql'

# create_hive_raw_tables_task = BashOperator(
#     task_id='create_hive_raw_tables',
#     bash_command=f"""
#     export JAVA_HOME=/usr/lib/jvm/temurin-8-jdk-amd64
#     export PATH=$JAVA_HOME/bin:$PATH
    
#     export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
#     export HIVE_CONF_DIR=/opt/hive/conf
    
#     echo "Executing HQL script"
#     hive -f "{HIVE_RAW_DDL_SCRIPT}"
#     """,
#     dag=dag
# )

# # Add new partition to metastore
# msc_repair_task = BashOperator(
#     task_id='msc_repair_task',
#     bash_command="""
#     export JAVA_HOME=/usr/lib/jvm/temurin-8-jdk-amd64
#     export PATH=$JAVA_HOME/bin:$PATH
    
#     export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
#     export HIVE_CONF_DIR=/opt/hive/conf
    
#     hive -e "
#     USE bronze_db;
#     MSCK REPAIR TABLE weather_data;
#     MSCK REPAIR TABLE financial_data;
#     "
#     """
# )

# start_task >> [ingest_weather_task, ingest_stock_task]

# [ingest_weather_task, ingest_stock_task] >> validate_data_task

# validate_data_task >> upload_to_hdfs_task

# upload_to_hdfs_task >> create_hive_raw_tables_task

# create_hive_raw_tables_task >> msc_repair_task