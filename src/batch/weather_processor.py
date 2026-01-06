from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from utils.logger_config import logger
import os

class WeatherBatchProccessor:
    def __init__(self):
        self.spark = self.create_spark_session()
        
    def create_spark_session(self):
        return SparkSession.builder \
            .appName("DailyBatchProcessor") \
            .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
            .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
            .enableHiveSupport() \
            .getOrCreate()   
     
    def compute(self, compute_type, **context):
        """Compute daily and monthly metrics"""
        
        start = context["data_interval_start"]
        batch_year = start.year
        batch_month = start.month
        batch_day = start.day
        
        if compute_type == "daily":
            silver_path_threshold = f"{os.getenv('SILVER_LAYER_PATH')}/weather/threshold_anomalies/year={batch_year}/month={batch_month}/day={batch_day}"
            silver_path_change = f"{os.getenv('SILVER_LAYER_PATH')}/weather/change_anomalies/year={batch_year}/month={batch_month}/day={batch_day}"
        else:
            silver_path_threshold = f"{os.getenv('SILVER_LAYER_PATH')}/weather/threshold_anomalies/year={batch_year}/month={batch_month}"
            silver_path_change = f"{os.getenv('SILVER_LAYER_PATH')}/weather/change_anomalies/year={batch_year}/month={batch_month}"
        
        df_sil_threshold = self.spark \
            .read.parquet(silver_path_threshold)
        
        df_sil_change = self.spark \
            .read.parquet(silver_path_change)
        
        logger.info(f"threshold count = {df_sil_threshold.count()}")
        logger.info(f"change count = {df_sil_change.count()}")

        threshold_anomalies = df_sil_threshold \
            .groupBy("city", "country") \
            .agg(
                round(avg(col("temperature").cast("double")), 2).alias("avg_temperature"),
                round(avg(col("pressure").cast("double")), 2).alias("avg_pressure"),
                round(avg(col("humidity").cast("double")), 2).alias("avg_humidity"),
                round(avg(col("visibility").cast("double")), 2).alias("avg_visibility"),
                round(avg(col("wind_speed").cast("double")), 2).alias("avg_wind_speed"),
                max("temp_anomaly").cast("int").alias("has_temp_anomaly"),
                max("pressure_anomaly").cast("int").alias("has_pressure_anomaly"),
                max("visibility_anomaly").cast("int").alias("has_visibility_anomaly"),
                max("wind_anomaly").cast("int").alias("has_wind_anomaly")
            ) \
            .withColumn("anomaly_score", (col("has_temp_anomaly") + col("has_pressure_anomaly")
                        + col("has_visibility_anomaly") + col("has_wind_anomaly")).cast("int")) \
            .withColumn("year", lit(batch_year).cast("int")) \
            .withColumn("month", lit(batch_month).cast("int"))
            
        change_anomalies = df_sil_change \
            .withColumn("temp_change_flag", col("temp_change_anomaly").isNotNull()) \
            .withColumn("pressure_change_flag", col("pressure_change_anomaly").isNotNull()) \
            .withColumn("wind_change_flag", col("wind_change_anomaly").isNotNull()) \
            .withColumn("humidity_change_flag", col("humidity_change_anomaly").isNotNull()) \
            .withColumn("visibility_change_flag", col("visibility_change_anomaly").isNotNull()) \
            .groupBy("city", "country") \
            .agg(
                count("*").cast("int").alias("total_windows"),
                max("temp_rise").cast("double").alias("max_temp_rise"),
                max("temp_fall").cast("double").alias("max_temp_fall"),
                max("wind_rise").cast("double").alias("max_wind_rise"),
                max("pressure_drop").cast("double").alias("max_pressure_drop"),
                max("humidity_rise").cast("double").alias("max_humidity_rise"),
                min("min_visibility").cast("double").alias("lowest_visibility"),
                max("temp_change_flag").cast("int").alias("has_temp_change"),
                max("pressure_change_flag").cast("int").alias("has_pressure_change"),
                max("wind_change_flag").cast("int").alias("has_wind_change"),
                max("humidity_change_flag").cast("int").alias("has_humidity_change"),
                max("visibility_change_flag").cast("int").alias("has_visibility_change")
            ) \
            .withColumn("anomaly_score", (col("has_temp_change") + col("has_pressure_change")
                        + col("has_wind_change") + col("has_humidity_change") + col("has_visibility_change")).cast("int")) \
            .withColumn("year", lit(batch_year).cast("int")) \
            .withColumn("month", lit(batch_month).cast("int"))
        
        if compute_type == "daily":
            change_anomalies = change_anomalies.withColumn("day", lit(batch_day).cast("int"))
            threshold_anomalies = threshold_anomalies.withColumn("day", lit(batch_day).cast("int"))

        if threshold_anomalies.rdd.isEmpty():
            logger.warning("No threshold anomalies data to write to gold layer")
            return False
        
        if change_anomalies.rdd.isEmpty():
            logger.warning("No change anomalies data to write to gold layer")
            return False
        
        self.spark.sql("""
        CREATE DATABASE IF NOT EXISTS weather
        LOCATION 'hdfs://namenode:9000/warehouse/weather.db';
        """)
        
        gold_path = f"{os.getenv('GOLD_LAYER_PATH')}/weather"
         
        if compute_type == "daily":
            threshold_anomalies.write \
                .mode("overwrite") \
                .partitionBy(["year", "month", "day"]) \
                .parquet(f"{gold_path}/daily/threshold_anomalies")
                
            change_anomalies.write \
                .mode("overwrite") \
                .partitionBy(["year", "month", "day"]) \
                .parquet(f"{gold_path}/daily/change_anomalies")
        
            logger.info("Weather daily written to gold layer")
            
            self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS weather.daily_threshold_anomalies(
                city STRING,
                country STRING,
                avg_temperature DOUBLE,
                avg_pressure DOUBLE,
                avg_humidity DOUBLE,
                avg_visibility DOUBLE,
                avg_wind_speed DOUBLE,
                has_temp_anomaly INT,
                has_pressure_anomaly INT,
                has_visibility_anomaly INT,
                has_wind_anomaly INT,
                anomaly_score INT
            )  
            USING PARQUET
            PARTITIONED BY (year INT, month INT, day INT)
            LOCATION '{gold_path}/daily/threshold_anomalies'
            """)
            
            self.spark.sql("MSCK REPAIR TABLE weather.daily_threshold_anomalies")
            
            logger.info("weather.daily_threshold_anomalies table created in Hive")
            
            self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS weather.daily_change_anomalies(
                city STRING,
                country STRING,
                total_windows INT,
                max_temp_rise DOUBLE,
                max_temp_fall DOUBLE,
                max_wind_rise DOUBLE,
                max_pressure_drop DOUBLE,
                max_humidity_rise DOUBLE,
                lowest_visibility DOUBLE,
                has_temp_change INT,
                has_pressure_change INT,
                has_wind_change INT,
                has_humidity_change INT,
                has_visibility_change INT,
                anomaly_score INT
            )  
            USING PARQUET
            PARTITIONED BY (year INT, month INT, day INT)
            LOCATION '{gold_path}/daily/change_anomalies'
            """)
            
            self.spark.sql("MSCK REPAIR TABLE weather.daily_change_anomalies")
            
            logger.info("weather.daily_change_anomalies table created in Hive")
        else:
            threshold_anomalies.write \
                .mode("overwrite") \
                .partitionBy(["year", "month"]) \
                .parquet(f"{gold_path}/monthly/threshold_anomalies")
                
            change_anomalies.write \
                .mode("overwrite") \
                .partitionBy(["year", "month"]) \
                .parquet(f"{gold_path}/monthly/change_anomalies")
        
            logger.info("Weather monthly data written to gold layer")
            
            self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS weather.monthly_threshold_anomalies(
                city STRING,
                country STRING,
                avg_temperature DOUBLE,
                avg_pressure DOUBLE,
                avg_humidity DOUBLE,
                avg_visibility DOUBLE,
                avg_wind_speed DOUBLE,
                has_temp_anomaly INT,
                has_pressure_anomaly INT,
                has_visibility_anomaly INT,
                has_wind_anomaly INT,
                anomaly_score INT
            )  
            USING PARQUET
            PARTITIONED BY (year INT, month INT)
            LOCATION '{gold_path}/monthly/threshold_anomalies'
            """)
            
            self.spark.sql("MSCK REPAIR TABLE weather.monthly_threshold_anomalies")
            
            logger.info("weather.monthly_threshold_anomalies table created in Hive")
            
            self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS weather.monthly_change_anomalies(
                city STRING,
                country STRING,
                total_windows INT,
                max_temp_rise DOUBLE,
                max_temp_fall DOUBLE,
                max_wind_rise DOUBLE,
                max_pressure_drop DOUBLE,
                max_humidity_rise DOUBLE,
                lowest_visibility DOUBLE,
                has_temp_change INT,
                has_pressure_change INT,
                has_wind_change INT,
                has_humidity_change INT,
                has_visibility_change INT,
                anomaly_score INT
            )  
            USING PARQUET
            PARTITIONED BY (year INT, month INT)
            LOCATION '{gold_path}/monthly/change_anomalies'
            """)
            
            self.spark.sql("MSCK REPAIR TABLE weather.monthly_change_anomalies")
            
            logger.info("weather.monthly_change_anomalies table created in Hive")
        
        return True
    
    def run_batch_processing(self, compute_type, **context):
        start = context["data_interval_start"]
        
        if compute_type not in ["daily", "monthly"]:
            raise ValueError("compute_type must be daily or monthly")
            
        logger.info(f"Starting {compute_type} weather batch processing for data interval start: {start}")
        
        gold_result = self.compute(compute_type, **context)
        
        if gold_result:
            logger.info("Silver to gold transformation completed successfully")
        else:
            logger.warning("Silver to gold transformation completed with warnings")
        
        logger.info(f"{compute_type} weather batch processing pipeline completed successfully")
        
        return gold_result
            
def daily_task(**context):
    """Airflow task for daily weather batch processing pipeline"""
    
    processor = WeatherBatchProccessor()
    
    try:
        return processor.run_batch_processing("daily", **context)
    except Exception as e:
        logger.exception(f"Weather batch daily task failed: {e}")
        raise
    finally:
        processor.spark.stop()
        logger.info("Spark session stopped")
        
def monthly_task(**context):
    """Airflow task for monthly weather batch processing pipeline"""
    
    processor = WeatherBatchProccessor()
    
    try:
        return processor.run_batch_processing("monthly", **context)
    except Exception as e:
        logger.exception(f"Weather batch monthly task failed: {e}")
        raise
    finally:
        processor.spark.stop()
        logger.info("Spark session stopped")
            
