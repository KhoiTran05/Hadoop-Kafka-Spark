import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.avro.functions import from_avro
from utils.logger_config import logger
import requests

class WeatherStreamProcessor:
    def __init__(self):
        self.spark = self.create_spark_session()
        #os.getenv("KAFKA_BROKER_URL")
        self.kafka_servers = 'localhost:9092'
        
    def create_spark_session(self):
        # .config("spark.sql.streaming.checkpointLocation", "hdfs://namenode:9000/checkpoints/weather")
        return SparkSession.builder \
            .appName("LiveMatchesStreamProcessor") \
            .master("local[*]") \
            .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.5.5,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5") \
            .getOrCreate()
            
    def get_schema(self, topic):
        # os.getenv("SCHEMA_REGISTRY_URL")
        schema_registry = 'http://localhost:8081'
        try:
            response = requests.get(
                f"{schema_registry}/subjects/{topic}-value/versions/latest/schema"
            )
            response.raise_for_status()
            
            logger.info(f"Successfully get '{topic}' schema from Schema Registry")
            
            avro_schema = response.text
            return avro_schema
        except Exception as e:
            logger.error(f"Failed to get schema for {topic}: {str(e)}")
            return None
        
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
                .withColumn("event_timestamp", to_timestamp(from_unixtime(col("dt")))) 
                
        return df_avro
    
    def write_stream_to_console(self, df, output_mode, checkpoint_location):
        try:
            logger.info("Start writing stream to console ...")
            
            query = df.writeStream \
                .outputMode(output_mode) \
                .format("console") \
                .option("truncate", False) \
                .option("checkpointLocation", f"./checkpoints/weather/{checkpoint_location}") \
                .start()
                
            query.awaitTermination()
            
        except KeyboardInterrupt:
            logger.info("Streaming stopped by user")
            
            query.stop() 
            logger.info("Streaming query stopped successfully.")
        except Exception as e:
            logger.error("Failed to write stream to console")
    
    def write_to_alert_topic(self, df, topic, checkpoint_location):           
        try:
            logger.info(f"Start writing stream to '{topic}' topic ...")
            
            query = df \
                .selectExpr(
                    "CAST(NULL AS STRING) AS key",
                    "CAST(to_json(struct(*)) AS STRING) AS value"
                ) \
                .writeStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "localhost:9092") \
                .option("topic", topic) \
                .option("checkpointLocation", f"./checkpoints/weather/{checkpoint_location}") \
                .start()
                
            return query
            
        except Exception as e:
            logger.error(f"Failed to write to '{topic}' topic")
            
    def threshold_based_anomaly_detect(self, df):
        """
        Detect weather anomalies using threshold
        Args:
            df (DataFrame): Raw dataframe 

        Returns:
            DataFrame: Dataframe with anomaly features
        """
        
        anomaly = df \
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
                
        result = anomaly.filter(
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
        df = df.withWatermark("event_timestamp", "2 minutes")
        
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
            
        filtered = anomaly.filter(
            col("temp_change_anomaly").isNotNull() |
            col("pressure_change_anomaly").isNotNull() |
            col("wind_change_anomaly").isNotNull() |
            col("humidity_change_anomaly").isNotNull() |
            col("visibility_change_anomaly").isNotNull()
        )
        
        result = filtered.select(
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
    
    
    def start_weather_stream_pipeline(self):
        logger.info("Start weather streaming pipeline ...")
        
        topic = 'weather'
        schema = self.get_schema(topic)
        df_raw = self.read_kafka_stream(topic, schema)
        
        threshold_detect_df = self.threshold_based_anomaly_detect(df_raw)
        threshold_query = self.write_to_alert_topic(threshold_detect_df, 'weather-alert', 'threshold_detect')
        
        change_detect_df = self.change_anomaly_detect(df_raw)
        change_query = self.write_to_alert_topic(change_detect_df, 'weather-alert', 'change_detect')
        
        self.spark.streams.awaitAnyTermination()

def main():
    processor = WeatherStreamProcessor()
    processor.start_weather_stream_pipeline()
    
if __name__ == '__main__':
    main()