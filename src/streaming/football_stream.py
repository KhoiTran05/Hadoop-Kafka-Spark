import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.avro.functions import from_avro
from utils.logger_config import logger
import requests

class FootballStreamProcessor:
    def __init__(self):
        self.spark = self.create_spark_session()
        self.kafka_servers = os.getenv("KAFKA_BROKER_URL")
        
    def create_spark_session(self):
        return SparkSession.builder \
            .appName("LiveFootballStreamProcessor") \
            .config("spark.sql.streaming.checkpointLocation", "hdfs://namenode:9000/checkpoints/football") \
            .getOrCreate()
            
    def get_schema(self, topic):
        schema_registry = os.getenv("SCHEMA_REGISTRY_URL")
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
                .withColumn(
                    "event_timestamp", 
                    to_timestamp(col("utcDate"), "yyyy-MM-dd'T'HH:mm:ssX")
                ) 
                
        return df_avro
    
    def write_stream_to_console(self, df, output_mode, checkpoint_location):
        try:
            logger.info("Start writing stream to console ...")
            
            query = df.writeStream \
                .outputMode(output_mode) \
                .format("console") \
                .option("truncate", False) \
                .option("checkpointLocation", f"hdfs://namenode:9000/checkpoints/football/{checkpoint_location}") \
                .start()
                
            query.awaitTermination()
            
        except KeyboardInterrupt:
            logger.info("Streaming stopped by user")
            
            query.stop() 
            logger.info("Streaming query stopped successfully.")
        except Exception as e:
            logger.error("Failed to write stream to console")
            
    def write_to_kakfa(self, df, topic, checkpoint_location):           
        try:
            logger.info(f"Start writing stream to '{topic}' topic ...")
            
            query = df \
                .selectExpr(
                    "CAST(id AS STRING) AS key",
                    "CAST(to_json(struct(*)) AS STRING) AS value"
                ) \
                .writeStream \
                .format("kafka") \
                .outputMode("append") \
                .option("kafka.bootstrap.servers", self.kafka_servers) \
                .option("topic", topic) \
                .option("checkpointLocation", f"hdfs://namenode:9000/checkpoints/football/{checkpoint_location}") \
                .start()
            
            return query
        except KeyboardInterrupt:
            logger.info("Streaming stopped by user")
            
            query.stop() 
            logger.info("Streaming query stopped successfully.")
            
        except Exception as e:
            logger.exception(f"Failed to write to '{topic}' topic")
            
    def process_data(self, df):
        df = df \
            .withWatermark("event_timestamp", "5 minutes") \
            .dropDuplicates(["id", "lastUpdated"]) \
            .filter(col("homeTeam").isNotNull() & col("awayTeam").isNotNull()) \
            .select(
                "id",
                "event_timestamp",
                "status",
                col("homeTeam.id").alias("home_team_id"), 
                col("homeTeam.name").alias("home_team_name"), 
                col("homeTeam.crest").alias("home_team_crest"),
                col("awayTeam.id").alias("away_team_id"), 
                col("awayTeam.name").alias("away_team_name"), 
                col("awayTeam.crest").alias("away_team_crest"),
                "score.*"
            )
            
        return df
    
    def start_football_stream_pipeline(self):
        schema = self.get_schema("football_live")
        
        df = self.read_kafka_stream("football_live", schema)
        processed_df = self.process_data(df)
        
        query = self.write_to_kakfa(processed_df, "processed-matches", "processed")
        query.awaitTermination()
        
        return True
    
def main():
    processor = FootballStreamProcessor()
    
    try:
        return processor.start_football_stream_pipeline()
    except Exception:
        logger.exception("Error during football stream pipeline execution")
        raise
    
if __name__ == '__main__':
    main()
    
        