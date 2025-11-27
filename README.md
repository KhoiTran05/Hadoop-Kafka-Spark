# Hadoop Ecosystem Real Data Pipeline

## Project Description
This project implements a big data pipeline designed to handle both real-time streaming and batch processing requirements for football and weather data. The core of the system is Apache Kafka, serving as the central nervous system (Event Bus) that decouples data ingestion from processing.

The solution integrates HDFS, Hive, Spark (Streaming & Batch), Kafka, and Airflow to create a robust platform for:

- **Real-time**: Instant weather alerts and live Premier League football dashboards.
- **Batch**: Historical analysis, complex ETL, and long-term data warehousing.

## Data Source
- **Type**: Multi-source real-world data streams
- **Sources**:
  - Football APIs: football-data.org
  - Weather APIs: OpenWeatherMap
- **Format**: JSON (Raw ingestion), Parquet (Storage), Avro (Schema Registry)
- **Volume**: High-frequency polling for real-time layers
- **Update Frequency**:
  - **Real-time**: Every 5-60 seconds (Streaming)
  - **Batch**: Hourly/Daily consolidation
- **Data Retention**: 
  - **Kafka**: 7 days (Buffer)
  - **HDFS/Hive**: 5 years (Historical Source of Truth)

## Tech Stack
- **Storage**: Apache Hadoop HDFS 3.3.6 with tiered storage
- **Data Warehouse**: Apache Hive 2.3.2 (Metastore on PostgreSQL)
- **Event Backbone**: Apache Kafka 7.4.0 (KRaft/Zookeeper mode) + Schema Registry
- **Processing Engine**: Spark 3.5.0 (Unified engine for both Batch & Structured Streaming)
- **Orchestration**: Apache Airflow 2.8.4
- **Real-time Store**: Redis 7 (Caching & Dashboard backend)
- **Database**: PostgreSQL 13 (Metadata & Airflow backend)
- **Monitoring**: Kafka UI, Spark UI, HDFS Web UI
- **Container**: Docker & Docker Compose

## Project Purpose
- Handle real-world data complexity and irregularities
- Implement robust error handling and data validation
- Learn data ingestion patterns from various APIs
- Practice large-scale data processing with HDFS
- Understand data quality and cleansing processes
- Implement automated data pipeline orchestration


