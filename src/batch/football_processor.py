from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import os
from datetime import datetime, timedelta
from utils.logger_config import logger

class FootballWeeklyBatchProcessor:
    def __init__(self):
        self.spark = self.create_spark_session()
        
    def create_spark_session(self):
        return SparkSession.builder \
            .appName("DailyBatchProcessor") \
            .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
            .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
            .enableHiveSupport() \
            .getOrCreate()
    
    def bronze_to_silver(self, initial_load=False, **context):
        """Read data from bronze layer and transform for silver layer"""
        logger.info("Processing football bronze to silver")
        
        execution_date = context["execution_date"]
        batch_year = execution_date.year
        batch_month = f"{execution_date.month:02d}"
        batch_day = f"{execution_date.day:02d}"
        
        if initial_load:
            bronze_path = f"{os.getenv('BRONZE_LAYER_PATH')}/football"
        else:
            bronze_path = f"{os.getenv('BRONZE_LAYER_PATH')}/football/batch_year={batch_year}/batch_month={batch_month}/batch_day={batch_day}"
        
        df_bronze = self.spark.read.parquet(bronze_path)
        
        logger.info(f"Read {df_bronze.count()} records from bronze layer")
        
        df_silver = df_bronze \
            .withColumn("match_timestamp", to_timestamp(col("date"))) \
            .withColumn("season_start_date", to_date(col("season_start_date"))) \
            .withColumn("season_end_date", to_date(col("season_end_date"))) \
            .filter(col("id").isNotNull()) \
            .dropDuplicates("id") \
            .withColumn("year", year(col("match_timestamp"))) \
            .withColumn("month", month(col("match_timestamp"))) \
            .withColumn("day", dayofmonth(col("match_timestamp"))) \
            .withColumn("processed_at", current_timestamp())
            
        df_silver = df_silver \
            .withColumn("is_home_win", when(col("winner") == "HOME_TEAM", True).otherwise(False)) \
            .withColumn("is_away_win", when(col("winner") == "AWAY_TEAM", True).otherwise(False)) \
            .withColumn("is_draw", when(col("winner") == "DRAW", True).otherwise(False)) \
            .withColumn("total_goals", col("fulltime_home") + col("fulltime_away")) 
        
        silver_path = f"{os.getenv('SILVER_LAYER_PATH')}/football"
        
        try:
            df_silver.write \
                .mode("append") \
                .partitionBy("year", "month", "day") \
                .parquet(silver_path)
                
        except Exception:
            logger.exception(f"Failed writing silver for {batch_year}-{batch_month}-{batch_day}")
            raise 
        
        logger.info(f"Data written to silver layer: {silver_path}")
                
        
    def read_silver_layer_data(self, start_ts=None, end_ts=None):
        """Read data from silver layer for processing"""
        silver_path = f"{os.getenv('SILVER_LAYER_PATH')}/football"
        
        if start_ts and end_ts:
            df = self.spark.read \
                .parquet(silver_path) \
                .filter(col("match_timestamp").between(start_ts, end_ts))
        else:
            df = self.spark.read \
                .parquet(silver_path)
        
        return df
        
    def compute_team_stats(self, df_silver):
        """Compute teams statistics"""
        gold_path = f"{os.getenv('GOLD_LAYER_PATH')}/football"
        
        df_home_stats = df_silver \
            .select(
                col("home_team_id").alias("team_id"),
                col("home_team_name").alias("team_name"),
                col("match_timestamp"),
                col("matchday"),
                when(col("is_home_win"), 1).otherwise(0).alias("wins"),
                when(col("is_draw"), 1).otherwise(0).alias("draws"),
                when(col("is_away_win"), 1).otherwise(0).alias("losses"),
                col("fulltime_home").alias("goals_for"),
                col("fulltime_away").alias("goals_against"),
                (col("fulltime_home") - col("fulltime_away")).alias("goals_difference"),
                when(col("fulltime_away") == 0, 1).otherwise(0).alias("clean_sheets"),
                when(col("is_home_win") & (col("halftime_home") < col("halftime_away")), 1).otherwise(0).alias("comeback_wins"),
                lit(1).alias("matches"),
                lit(1).alias("home_matches"),
                lit(0).alias("away_matches")
            )
            
        df_away_stats = df_silver \
            .select(
                col("away_team_id").alias("team_id"),
                col("away_team_name").alias("team_name"),
                col("match_timestamp"),
                col("matchday"),
                when(col("is_away_win"), 1).otherwise(0).alias("wins"),
                when(col("is_draw"), 1).otherwise(0).alias("draws"),
                when(col("is_home_win"), 1).otherwise(0).alias("losses"),
                col("fulltime_away").alias("goals_for"),
                col("fulltime_home").alias("goals_against"),
                (col("fulltime_away") - col("fulltime_home")).alias("goals_difference"),
                when(col("fulltime_home") == 0, 1).otherwise(0).alias("clean_sheets"),
                when(col("is_away_win") & (col("halftime_away") < col("halftime_home")), 1).otherwise(0).alias("comeback_wins"),
                lit(1).alias("matches"),
                lit(1).alias("away_matches"),
                lit(0).alias("home_matches")
            )
        
        df_match_day_stats = df_home_stats.union(df_away_stats) \
            .groupBy("matchday", "team_id", "team_name") \
            .agg(
                sum("matches").alias("matches_played"),
                sum("home_matches").alias("home_matches_played"),
                sum("away_matches").alias("away_matches_played"),
                sum("wins").alias("wins"),
                sum("losses").alias("losses"),
                sum("draws").alias("draws"),
                sum("goals_for").alias("goals_for"),
                sum("goals_against").alias("goals_against"),
                sum("goals_difference").alias("goals_difference"),
                sum("clean_sheets").alias("clean_sheets"),
                sum("comeback_wins").alias("comeback_wins"),
                max("match_timestamp").alias("latest_match_date")
            ) 
        
        window_spec = Window.partitionBy("team_id").orderBy("matchday") \
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
            
        df_team_stats = df_match_day_stats \
            .withColumn("matches_played", sum("matches_played").over(window_spec)) \
            .withColumn("home_matches_played", sum("home_matches_played").over(window_spec)) \
            .withColumn("away_matches_played", sum("away_matches_played").over(window_spec)) \
            .withColumn("wins", sum("wins").over(window_spec)) \
            .withColumn("losses", sum("losses").over(window_spec)) \
            .withColumn("draws", sum("draws").over(window_spec)) \
            .withColumn("goals_for", sum("goals_for").over(window_spec)) \
            .withColumn("goals_against", sum("goals_against").over(window_spec)) \
            .withColumn("goals_difference", sum("goals_difference").over(window_spec)) \
            .withColumn("clean_sheets", sum("clean_sheets").over(window_spec)) \
            .withColumn("comeback_wins", sum("comeback_wins").over(window_spec)) \
            .withColumn("latest_match_date", max("latest_match_date").over(window_spec)) \
            .withColumn("points", col("wins") * 3 + col("draws")) \
            .withColumn("avg_goals", 
                        round(when(col("matches_played") > 0, col("goals_for") / col("matches_played")), 2)) \
            .withColumn("win_rate", round(col("wins") / col("matches_played") * 100, 2)) \
            .withColumn("updated_at", current_timestamp())
        
        df_team_stats \
            .write \
            .mode("overwrite") \
            .partitionBy("matchday") \
            .parquet(f"{gold_path}/team_statistics")
        
        logger.info("Team statistics data written to gold layer")
        
        self.spark.sql(f"""
        CREATE TABLE IF NOT EXISTS football.gold_team_statistics (
            match_day INT,
            team_id BIGINT,
            team_name STRING,
            matches_played INT,
            home_matches_played INT,
            away_matches_played INT,
            wins INT,
            losses INT,
            draws INT,
            goals_for INT,
            goals_against INT,
            goals_difference INT,
            clean_sheets INT,
            comeback_wins INT,
            latest_match_date TIMESTAMP,
            points INT,
            avg_goals DOUBLE,
            win_rate DOUBLE,
            updated_at TIMESTAMP
        )
        PARTITION BY (matchday INT)
        STORED AS PARQUET
        LOCATION '{gold_path}/team_statistics'
        """)
        
        self.spark.sql("MSCK REPAIR TABLE football.gold_team_statistics")
        
        logger.info("Team statistics table created in Hive")
        
        return True
        
    def compute_form_streaks(self, df_silver):
        """Compute teams form streaks on last 5 matches"""
        gold_path = f"{os.getenv('GOLD_LAYER_PATH')}/football"
        
        df_matches = df_silver.select(
            col("home_team_id").alias("team_id"),
            col("home_team_name").alias("team_name"),
            col("match_timestamp"),
            when(col("is_home_win"), "W")
                .when(col("is_away_win"), "L")
                .otherwise("D").alias("result"),
            col("fulltime_home").alias("goals_for"),
            col("fulltime_away").alias("goals_against"),
        ).union(
            df_silver.select(
                col("away_team_id").alias("team_id"),
                col("away_team_name").alias("team_name"),
                col("match_timestamp"),
                when(col("is_away_win"), "W")
                    .when(col("is_home_win"), "L")
                    .otherwise("D").alias("result"),
                col("fulltime_away").alias("goals_for"),
                col("fulltime_home").alias("goals_against"),
            )
        )
        
        window_spec = Window.partitionBy("team_id").orderBy(desc("match_timestamp"))
        
        df_form_streaks = df_matches \
            .withColumn("rn", row_number().over(window_spec)) \
            .filter(col("rn") <= 5) \
            .groupBy("team_id", "team_name") \
            .agg(
                concat_ws("-", collect_list("result")).alias("last_5_streaks"),
                sum("goals_for").alias("goals_last_5"),
                sum("goals_against").alias("conceded_last_5")
            ) \
            .withColumn("updated_at", current_timestamp())
            
        df_form_streaks.write \
            .mode("overwrite") \
            .parquet(f"{gold_path}/form_streaks")
            
        logger.info("Form streaks data written to gold layer")
            
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS football.form_streaks (
                team_id BIGINT,
                team_name STRING,
                last_5_streaks STRING,
                goals_last_5 INT,
                conceded_last_5 INT,
                updated_at TIMESTAMP
            )
            STORED AS PARQUET
            LOCATION '{gold_path}/form_streaks'
        """)
        
        logger.info("Form streaks table created in Hive")
        
        return True
    
    def compute_head_to_head(self, df_silver):
        """Compute teams head to head"""
        
        gold_path = f"{os.getenv('GOLD_LAYER_PATH')}/football"
        
        df_matchup = df_silver.select(
            col("home_team_id").alias("team_id"),
            col("home_team_name").alias("team_name"),
            col("away_team_id").alias("opponent_team_id"),
            col("away_team_name").alias("opponent_team_name"),
            when(col("is_home_win"), 1).otherwise(0).alias("wins"),
            when(col("is_draw"), 1).otherwise(0).alias("draws"),
            when(col("is_away_win"), 1).otherwise(0).alias("losses"),
            col("fulltime_home").alias("goals_for"),
            col("fulltime_away").alias("goals_against"),
            lit(1).alias("matches"),
            col("match_timestamp")
        ).union(
            df_silver.select(
                col("away_team_id").alias("team_id"),
                col("away_team_name").alias("team_name"),
                col("home_team_id").alias("opponent_team_id"),
                col("home_team_name").alias("opponent_team_name"),
                when(col("is_away_win"), 1).otherwise(0).alias("wins"),
                when(col("is_draw"), 1).otherwise(0).alias("draws"),
                when(col("is_home_win"), 1).otherwise(0).alias("losses"),
                col("fulltime_away").alias("goals_for"),
                col("fulltime_home").alias("goals_against"),
                lit(1).alias("matches"),
                col("match_timestamp")
            )
        ) \
        .groupBy("team_id", "team_name", "opponent_team_id", "opponent_team_name") \
        .agg(
            sum("matches").alias("matches"),
            sum("wins").alias("wins"),
            sum("losses").alias("losses"),
            sum("draws").alias("draws"),
            sum("goals_for").alias("goals_for"),
            sum("goals_against").alias("goals_against"),
            max("match_timestamp").alias("latest_match_timestamp")
        ) \
        .withColumn("win_rate", round(col("wins") / col("matches") * 100, 2))
        
        df_matchup.write \
            .mode("overwrite") \
            .parquet(f"{gold_path}/head_to_head")
        
        logger.info("Head to head data written to gold layer")
        
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS football.head_to_head (
                team_id BIGINT,
                team_name STRING,
                opponent_team_id BIGINT,
                opponent_team_name STRING,
                matches INT,
                wins INT,
                losses INT,
                draws INT,
                goals_for INT,
                goals_against INT,
                latest_match_timestamp TIMESTAMP,
                win_rate DOUBLE
            )
            STORED AS PARQUET
            LOCATION '{gold_path}/head_to_head'
        """
        )
        
        logger.info("Head to head table created in Hive")
        
        return True
    
    def silver_to_gold(self):
        """Read data from silver layer and transform for gold layer"""
        
        logger.info("Starting silver to gold transformation")
        
        df_silver = self.read_silver_layer_data()
        
        record_count = df_silver.count()
        logger.info(f"Read {record_count} records from silver layer")
        
        if record_count == 0:
            logger.warning("No records found in silver layer")
            return False
        
        team_stats_result = self.compute_team_stats(df_silver)
        form_streaks_result = self.compute_form_streaks(df_silver)
        head_to_head_result = self.compute_head_to_head(df_silver)
        
        processing_result = team_stats_result and form_streaks_result and head_to_head_result
        
        if processing_result:
            logger.info("Silver to gold transformation completed successfully")
        else:
            logger.error("Some gold layer transformations failed")
            
        return processing_result
        
    def run_weekly_batch_processing(self, initial_load=False, **context):
        """Run the complete weekly batch processing pipeline"""
        
        execution_date = context["execution_date"]
        logger.info(f"Starting weekly football batch processing for execution date: {execution_date}")
        
        logger.info("Processing bronze to silver layer")
        
        self.bronze_to_silver(initial_load=initial_load, **context)
        
        logger.info("Bronze to silver transformation completed")
        
        logger.info("Processing silver to gold layer")
        
        gold_result = self.silver_to_gold(**context)
        
        if gold_result:
            logger.info("Silver to gold transformation completed successfully")
        else:
            logger.warning("Silver to gold transformation completed with warnings")
        
        logger.info("Weekly football batch processing pipeline completed successfully")
        
        return gold_result

def initial_load_task(**context):
    """Airflow task for initial full data load"""
    logger.info("Initial load task started")
    
    processor = FootballWeeklyBatchProcessor()
    
    try:
        return processor.run_weekly_batch_processing(initial_load=True, **context)
    
    except Exception as e:
        logger.exception(f"Initial load task failed: {e}")
        raise
    
    finally:
        processor.spark.stop()
        logger.info("Spark session stopped")
        
def full_pipeline_task(**context):
    """Airflow task for complete weekly football batch processing pipeline"""
    logger.info("Full pipeline task started")
    
    processor = FootballWeeklyBatchProcessor()
    
    try:
        return processor.run_weekly_batch_processing(initial_load=False, **context)

    except Exception as e:
        logger.exception(f"Pipeline task failed: {e}")
        raise
    
    finally:
        processor.spark.stop()
        logger.info("Spark session stopped")