from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField

class LiveMatchesStreamProcessor:
    def __init__(self):
        self.spark = self.create_spark_session()
        
    def create_spark_session(self):
        return SparkSession.builder \
            .appName("LiveMatchesStreamProcessor") \
            .config("spark.sql.streaming.checkpointLocation", "hdfs://namenode:9000/checkpoints/live_football") \
            .getOrCreate()
            