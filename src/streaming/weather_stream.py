import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.avro.functions import from_avro
from utils.logger_config import logger
import requests

class WeatherStreamProcessor:
    def __init__(self):
        self.spark = self.create_spark_session()
        self.kafka_servers = os.getenv("KAFKA_BROKER_URL")
        
    def create_spark_session(self):
        return SparkSession.builder \
            .appName("WeatherStreamProcessor") \
            .config("spark.sql.streaming.checkpointLocation", "hdfs://namenode:9000/checkpoints/weather") \
            .getOrCreate()
            
    def get_schema(self, topic):
        schema_registry = os.getenv("SCHEMA_REGISTRY_URL")
        
        try:
            response = requests.get(
                f"{schema_registry}/subjects/{topic}-value/versions/latest/schema"
            )
            response.raise_for_status()
            logger.info(f"Successfully get '{topic}' schema from Schema Registry")
            
            return response.text
        except Exception as e:
            logger.exception(f"Failed to get schema for {topic}: {str(e)}")
            raise
        
    def read_kafka_stream(self, topic, schema):
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load()
                
        df_avro = df \
                .select(
                    col("timestamp").alias("kafka_timestamp"),
                    col("offset"),
                    col("partition"),
                    expr("substring(value, 6) as avro_value")
                ) \
                .withColumn("data", from_avro(col("avro_value"), schema)) \
                .select("data.*") \
                .withColumn(
                    "event_timestamp", 
                    from_utc_timestamp(to_timestamp(from_unixtime(col("dt"))), "Asia/Ho_Chi_Minh")
                ) \
                .withColumn("ingested_at", to_timestamp(col("ingested_at")))
                
        return df_avro
        
    def write_to_console(self, 
                         df, 
                         output_mode, 
                         checkpoint_suffix):
        logger.info("Start writing to console ...")
        
        query = None
        try:
            query = df.writeStream \
                .outputMode(output_mode) \
                .format("console") \
                .option("truncate", False) \
                .option("checkpointLocation", f"hdfs://namenode:9000/checkpoints/weather/{checkpoint_suffix}") \
                .start()
                
            query.awaitTermination()
            
            return query
        except KeyboardInterrupt:
            logger.info("Streaming stopped by user")
            query.stop() 
            logger.info("Streaming query stopped successfully.")
        except Exception as e:
            logger.error("Failed to write stream to console")
            
            if query:
                query.stop()
            raise
    
    def write_to_alert_topic(self, 
                             df, 
                             topic, 
                             checkpoint_suffix):       
        logger.info(f"Start writing to '{topic}' topic ...")    
        
        query = None
        try:
            query = df \
                .selectExpr(
                    "CAST(NULL AS STRING) AS key",
                    "CAST(to_json(struct(*)) AS STRING) AS value"
                ) \
                .writeStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_servers) \
                .option("topic", topic) \
                .option("checkpointLocation", f"hdfs://namenode:9000/checkpoints/weather/{checkpoint_suffix}") \
                .start()
                
            return query
        except Exception as e:
            logger.exception(f"Failed to write to '{topic}' topic")
            
            if query:
                query.stop()
            raise
    
    def write_to_bronze_layer(self, df):
        """Write raw weather data to HDFS Bronze Layer"""
        bronze_layer_path = f"{os.getenv('BRONZE_LAYER_PATH')}/weather"
        logger.info("Writting to HDFS bronze layer")
        
        query = None
        try:
            query = df.writeStream \
                .partitionBy("ingested_at") \
                .format("parquet") \
                .outputMode("append") \
                .option("path", bronze_layer_path) \
                .option("checkpointLocation", "hdfs://namenode:9000/checkpoints/weather/bronze") \
                .trigger(processingTime="10 minutes") \
                .start()

            return query
        except Exception as e:
            logger.exception("Failed to write to bronze layer")
            
            if query:
                query.stop()
            raise
        
    def write_to_silver_layer(self, 
                              df, 
                              dataset_name, 
                              checkpoint_suffix,
                              partition_cols=None, 
                              trigger="10 minutes"):
        """Write cleaned and enriched weather data to HDFS Silver Layer"""
        logger.info("Writting to HDFS silver layer")
        
        silver_layer_path = f"{os.getenv('SILVER_LAYER_PATH')}/weather/{dataset_name}"
        checkpoint_path = f"hdfs://namenode:9000/checkpoints/weather/silver/{checkpoint_suffix}"
        
        query = None
        try:
            writer = df.writeStream \
                .format("parquet") \
                .outputMode("append") \
                .option("path", silver_layer_path) \
                .option("checkpointLocation", checkpoint_path) \
                .trigger(processingTime=trigger) 
            
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
                
            query = writer.start()
            
            return query
        except Exception as e:
            logger.exception("Failed to write to silver layer")
            
            if query:
                query.stop()
            raise
    
    def threshold_based_anomaly_detect(self, df):
        """
        Detect weather anomalies using threshold
        Args:
            df (DataFrame): Raw dataframe 

        Returns:
            DataFrame: Dataframe with anomaly features
        """
        
        df_threshold_alerts = df \
            .select(
                "event_timestamp",
                lit("threshold").alias("alert_type"),
                "id",
                "city",
                "country",
                "weather_main",
                "weather_description",
                "temperature",
                "pressure",
                "humidity",
                "visibility",
                "wind_speed"
            ) \
            .withColumn("temp_anomaly", expr("CASE WHEN temperature <= 0 OR temperature >= 38 THEN True ELSE False END")) \
            .withColumn("pressure_anomaly", expr("CASE WHEN pressure <= 995 OR pressure >= 1033 THEN True ELSE False END")) \
            .withColumn("visibility_anomaly", expr("CASE WHEN visibility <= 1000 THEN True ELSE False END")) \
            .withColumn("wind_anomaly", expr("CASE WHEN wind_speed >= 10 THEN True ELSE False END")) \
        
        result = df_threshold_alerts.filter(
                (col("temp_anomaly") == True) |
                (col("pressure_anomaly") == True) |
                (col("visibility_anomaly") == True) |
                (col("wind_anomaly") == True) 
            )
        
        return result
                
    def change_anomaly_detect(self, df):
        """
        Detect weather anomalies on unexpected changes
        Args:
            df (DataFrame): Raw dataframe 

        Returns:
            DataFrame: Dataframe with anomaly features
        """
        
        agg_df = df \
            .groupBy(
                "city",
                "country",
                window("event_timestamp", "30 minutes", "5 minutes").alias("time_window")
            ) \
            .agg(
                avg("temperature").alias("avg_temp"),
                max(struct("temperature", "event_timestamp")).alias("max_temp_info"), 
                min(struct("temperature", "event_timestamp")).alias("min_temp_info"),
                
                avg("pressure").alias("avg_pressure"),
                max(struct("pressure", "event_timestamp")).alias("max_pressure_info"), 
                min(struct("pressure", "event_timestamp")).alias("min_pressure_info"),
            
                avg("wind_speed").alias("avg_wind_speed"), 
                max(struct("wind_speed", "event_timestamp")).alias("max_wind_info"), 
                min("wind_speed").alias("min_wind"), 
                
                avg("humidity").alias("avg_humidity"), 
                max(struct("humidity", "event_timestamp")).alias("max_humidity_info"), 
                min("humidity").alias("min_humidity"),
                
                avg("visibility").alias("avg_vis"),
                min(struct("visibility", "event_timestamp")).alias("min_vis_info")
            ) 
            
        analysis = agg_df \
            .withColumn("temp_rise", col("max_temp_info.temperature") - col("avg_temp")) \
            .withColumn("temp_fall", col("avg_temp") - col("min_temp_info.temperature")) \
            .withColumn("pressure_drop", col("max_pressure_info.pressure") - col("min_pressure_info.pressure")) \
            .withColumn("wind_rise", col("max_wind_info.wind_speed") - col("min_wind")) \
            .withColumn("humidity_rise", col("max_humidity_info.humidity") - col("min_humidity"))
            
        anomaly = analysis \
            .withColumn("temp_change_anomaly", when(col("temp_rise") >= 3, "rise rapidly")
                                    .when(col("temp_fall") >= 3, "fall rapidly")
                                    .otherwise(None)) \
            .withColumn("pressure_change_anomaly", when(col("pressure_drop") >= 2, "drop/unstable") 
                                        .otherwise(None)) \
            .withColumn("wind_change_anomaly", when(col("wind_rise") >= 8, "rise rapidly")
                                    .otherwise(None)) \
            .withColumn("humidity_change_anomaly", when(col("avg_humidity") == 0, None)
                                        .when((col("humidity_rise") / col("avg_humidity")) >= 0.2, "rise rapidly")
                                        .otherwise(None)) \
            .withColumn("visibility_change_anomaly", when((col("avg_vis") > 5000) & (col("min_vis_info.visibility") < 2000), "drop/unstable")
                                            .otherwise(None))
            
        
        result = anomaly \
            .filter(
                col("temp_change_anomaly").isNotNull() |
                col("pressure_change_anomaly").isNotNull() |
                col("wind_change_anomaly").isNotNull() |
                col("humidity_change_anomaly").isNotNull() |
                col("visibility_change_anomaly").isNotNull()
            ) \
            .select(
                lit("change").alias("alert_type"),
                col("city"),
                col("country"),
                col("time_window.start").alias("window_start"), 
                
                col("max_temp_info.temperature").alias("max_temp"),
                col("max_temp_info.event_timestamp").alias("max_temp_time"), 
                col("min_temp_info.temperature").alias("min_temp"),
                col("min_temp_info.event_timestamp").alias("min_temp_time"),
                col("temp_rise"),
                col("temp_fall"),
                col("temp_change_anomaly"),
                
                col("max_wind_info.wind_speed").alias("max_wind"),
                col("max_wind_info.event_timestamp").alias("max_wind_time"),
                col("wind_rise"),
                col("wind_change_anomaly"),
                
                col("min_pressure_info.pressure").alias("min_pressure"),
                col("min_pressure_info.event_timestamp").alias("min_pressure_time"),
                col("pressure_drop"),
                col("pressure_change_anomaly"),
                
                col("max_humidity_info.humidity").alias("max_humidity"),
                col("max_humidity_info.event_timestamp").alias("max_humid_time"),
                col("humidity_rise"),
                col("humidity_change_anomaly"),

                col("min_vis_info.visibility").alias("min_visibility"),
                col("min_vis_info.event_timestamp").alias("min_visibility_time"),
                col("visibility_change_anomaly")
            )
        
        return result
    
    def batch_process(self, batch_df, batch_id):
        """Process each micro-batch"""
        try:
            logger.info(f"Processing batch {batch_id} with {batch_df.count()} records")
            
            bronze_path = f"{os.getenv('BRONZE_LAYER_PATH')}/weather"
            
            batch_df.write \
                .mode("append") \
                .partitionBy("ingested_at") \
                .parquet(bronze_path)
                
            logger.info("Successfully wrote raw records to HDFS Bronze Layer")
            
        except Exception as e:
            logger.exception(f"Error processing batch {batch_id}")
            raise
        
    def start_weather_stream_pipeline(self):
        logger.info("Start weather streaming pipeline ...")
        
        schema = self.get_schema("weather")
        df_raw = self.read_kafka_stream("weather", schema)
        
        query = df_raw.writeStream \
            .foreachBatch(self.batch_process) \
            .option("checkpointLocation", "hdfs://namenode:9000/checkpoints/weather/main") \
            .trigger(processingTime="1 minute") \
            .start()
        
        df_base = df_raw \
            .withWatermark("event_timestamp", "5 minutes") \
            .dropDuplicates(["city", "event_timestamp"]) \
            .na.drop(subset=["city", "country", "event_timestamp"])
                
        df_threshold_alerts = self.threshold_based_anomaly_detect(df_base)
            
        query_threshold_alert = self.write_to_alert_topic(df_threshold_alerts, "weather-alert", "threshold_alert")
            
        df_threshold_silver = df_threshold_alerts \
            .withColumn("year", year(col("event_timestamp"))) \
            .withColumn("month", month(col("event_timestamp"))) \
            .withColumn("day", day(col("event_timestamp"))) \
            .withColumn("processed_at", current_timestamp())
            
        query_threshold_silver = self.write_to_silver_layer(
            df_threshold_silver, 
            "threshold_anomalies",
            "threshold_silver", 
            ["year", "month", "day"]
        )

        df_change_alerts = self.change_anomaly_detect(df_base)

        query_change_alert = self.write_to_alert_topic(df_change_alerts, "weather-alert", "change_alert")
            
        df_change_silver = df_change_alerts \
            .withColumn("year", year(col("window_start"))) \
            .withColumn("month", month(col("window_start"))) \
            .withColumn("day", day(col("window_start"))) \
            .withColumn("processed_at", current_timestamp())
            
        query_change_silver = self.write_to_silver_layer(
            df_change_silver, 
            "change_anomalies",
            "change_silver_v2", 
            ["year", "month", "day"]
        )

        logger.info("Weather streaming pipeline started successfully")
        self.spark.streams.awaitAnyTermination()
        
        return True
    
def main():
    processor = WeatherStreamProcessor()
    
    try:
        return processor.start_weather_stream_pipeline()
    except Exception:
        logger.exception("Error during weather stream pipeline execution")
        raise
    
if __name__ == '__main__':
    main()