# Hadoop Ecosystem Real Data Pipeline

## Project Description
This project implements a production-grade, enterprise-level big data pipeline using Apache Hadoop ecosystem with real-world datasets from multiple external APIs and sources. The comprehensive solution integrates HDFS, Hive, Pig, Sqoop, Spark, Kafka, and Airflow to create a scalable data platform for real-time and batch analytics on financial, weather, social media, and IoT data.

## Data Source
- **Type**: Multi-source real-world data streams
- **Sources**:
  - Financial APIs: Yahoo Finance, Alpha Vantage, Coinbase Pro
  - Weather APIs: OpenWeatherMap, WeatherAPI, NOAA
  - Social Media: Twitter API, Reddit API (via Pushshift)
  - IoT Simulation: Sensor data from simulated smart city infrastructure
  - Government Data: US Census, Economic indicators
  - News APIs: NewsAPI, Guardian API
- **Format**: JSON, CSV, Parquet, Avro, Protocol Buffers
- **Volume**: 10M+ records daily across all sources
- **Update Frequency**: Real-time streams + hourly/daily batch loads
- **Data Retention**: 5 years with automated archival

## Tech Stack
- **Storage**: Apache Hadoop HDFS 3.3.6 with tiered storage
- **Data Warehouse**: Apache Hive 3.1.3 with ACID transactions
- **ETL Processing**: Apache Pig 0.17.0, Apache Spark 3.5
- **Data Integration**: Apache Sqoop 1.4.7, Apache NiFi 1.23
- **Stream Processing**: Apache Kafka 2.8, Kafka Streams, Spark Streaming
- **Workflow Orchestration**: Apache Airflow 2.7.1
- **Database**: PostgreSQL 13 (metadata), Redis (caching)
- **Search & Analytics**: Apache Solr, Elasticsearch
- **Monitoring**: Prometheus, Grafana, Apache Atlas
- **Container**: Docker & Docker Compose with multi-stage builds
- **Languages**: Java, Scala (Spark), Python, SQL, HiveQL, Pig Latin

## Project Purpose
- Handle real-world data complexity and irregularities
- Implement robust error handling and data validation
- Learn data ingestion patterns from various APIs
- Practice large-scale data processing with HDFS
- Understand data quality and cleansing processes
- Implement automated data pipeline orchestration

## Setup Instructions

### Prerequisites
- Docker and Docker Compose installed
- At least 8GB RAM available for containers
- API keys for data sources (instructions provided)
- Java 8+ and Python 3.8+

### API Keys Setup
Create a `.env` file with your API keys:
```bash
# Weather API (free tier available)
OPENWEATHER_API_KEY=your_api_key_here

# Alpha Vantage for stocks (free tier available)
ALPHA_VANTAGE_API_KEY=your_api_key_here

# Optional: Quandl API for financial data
QUANDL_API_KEY=your_api_key_here
```

### Quick Start
```bash
# Navigate to project directory
cd hadoop-realdata-pipeline

# Set up environment variables
cp .env.example .env
# Edit .env with your API keys

# Start the complete data stack
docker-compose up -d

# Wait for all services to be ready
./scripts/wait-for-services.sh

# Run initial data ingestion
docker-compose exec airflow-scheduler python /scripts/ingest_all_data.py

# Execute the data processing pipeline
docker-compose exec spark-master bash /scripts/run_analysis.sh

# View results and monitoring
open http://localhost:9870  # HDFS Web UI
open http://localhost:8080  # Spark Master UI
open http://localhost:8081  # Airflow Web UI
```

### Data Sources Configuration
The pipeline automatically ingests data from:
1. **Stock Market**: Daily OHLCV data for S&P 500 companies
2. **Weather**: Hourly weather data for major US cities
3. **Census**: Population and demographic data
4. **Transportation**: NYC taxi trip records (sample)

## Architecture Diagram
```
[External APIs] → [Ingestion Layer] → [HDFS] → [Processing Layer] → [Analytics]
      ↓               ↓                 ↓            ↓              ↓
[Yahoo Finance]   [Python Scripts]  [Storage]   [Spark/MR]    [Dashboards]
[OpenWeather]     [Apache Flume]    [Raw Data]  [Validation]   [Reports]
[Census API]      [Airflow Jobs]    [Curated]   [Transform]    [Alerts]
[NYC OpenData]    [Error Handling]  [Archive]   [Analytics]    [Export]
```
## Project Structure
```
/your-data-platform/
├── .github/              # Hoặc .gitlab-ci.yml - CI/CD pipelines
│   └── workflows/
│       ├── build_spark.yml
│       └── deploy_airflow.yml
│
├── airflow/              # (Orchestration)
│   ├── dags/             # Nơi chứa các file Python định nghĩa DAGs
│   │   ├── dag_ingest_yahoo_finance.py
│   │   ├── dag_ingest_nyc_opendata.py
│   │   └── dag_process_daily_reports.py
│   ├── plugins/            # Các plugin custom cho Airflow
│   ├── config/             # Cấu hình Airflow (nếu cần)
│   └── scripts/            # Các shell scripts được gọi từ DAGs (nếu có)
│
├── common/               # Thư viện dùng chung
│   ├── python/             # (Python package)
│   │   ├── src/my_common_utils/
│   │   │   ├── __init__.py
│   │   │   ├── connectors.py (Kết nối DB, Kafka, HDFS)
│   │   │   └── transformations.py (Hàm transform chung)
│   │   └── setup.py
│   ├── scala/              # (Scala/Java library)
│   │   ├── src/main/scala/com/my_company/common/
│   │   └── build.sbt (hoặc pom.xml)
│   └── schemas/            # Định nghĩa schema (Avro, Parquet)
│
├── ingestion/            # (Ingestion Layer)
│   ├── nifi_templates/     # Các template của NiFi (nếu quản lý bằng code)
│   ├── sqoop_jobs/         # Các script .sh để chạy Sqoop import/export
│   ├── python_scripts/     # Các script Python (vd: dùng API)
│   │   ├── fetch_openweather.py
│   │   └── fetch_census_api.py
│   └── flume/              # Cấu hình Flume agents (nFume.conf)
│
├── processing/           # (Processing Layer - Core ETL/ELT)
│   ├── spark/              # Các job Spark (Scala & Python)
│   │   ├── src/main/scala/com/my_company/spark/
│   │   │   ├── ValidationJob.scala
│   │   │   └── AnalyticsJob.scala
│   │   ├── src/main/python/
│   │   │   ├── transform_users.py
│   │   │   └── validation_rules.py
│   │   ├── src/test/         # Unit tests cho Spark
│   │   ├── build.sbt         # Build config cho Scala
│   │   └── requirements.txt  # Dependencies cho PySpark
│   └── hive/               # (Data Warehouse)
│       ├── ddl/              # Data Definition Language
│       │   ├── 01_create_raw_tables.hql
│       │   ├── 02_create_curated_tables.hql
│       │   └── 03_create_views.hql
│       └── scripts/          # Các script HiveQL (chạy bởi Airflow)
│
├── streaming/            # (Stream Processing)
│   ├── kafka_streams/      # (Nếu dùng Kafka Streams)
│   │   ├── src/main/java/com/my_company/streams/
│   │   └── pom.xml
│   ├── spark_streaming/    # (Nếu dùng Spark Streaming)
│   │   └── src/main/scala/com/my_company/streaming/
│   └── producers/          # Các script producer (để test)
│
├── analytics/            # (Analytics Layer)
│   ├── elasticsearch/      # Cấu hình cho Elasticsearch
│   │   ├── mappings/
│   │   └── ingest_pipelines/
│   ├── solr/               # Cấu hình cho Solr
│   │   └── configsets/
│   └── dashboards/         # Export JSON của Grafana (để backup)
│
├── infrastructure/       # (Infra-as-Code & Monitoring)
│   ├── docker/             # (Dockerfile cho các service)
│   │   ├── base/           # (Base image)
│   │   ├── spark/          # (Dockerfile cho Spark, cài dependencies)
│   │   ├── airflow/        # (Dockerfile cho Airflow)
│   │   └── nifi/
│   ├── monitoring/
│   │   ├── prometheus/
│   │   │   ├── prometheus.yml
│   │   │   └── rules/
│   │   └── grafana/
│   │       ├── provisioning/
│   │       │   ├── datasources.yml
│   │       │   └── dashboards.yml
│   └── atlas/              # Cấu hình, definitions cho Apache Atlas
│
├── docs/                 # Tài liệu dự án
│   ├── architecture.md
│   ├── data_dictionary.md
│   └── setup_guide.md
│
├── .gitignore
├── docker-compose.yml    # File chính để chạy local dev environment
├── README.md             # Tổng quan dự án, cách setup
└── config/               # Cấu hình theo môi trường
    ├── dev.env
    ├── prod.env
```

## Data Pipeline Stages
1. **Ingestion**: API calls with rate limiting and error handling
2. **Validation**: Schema validation and data quality checks
3. **Storage**: Raw data stored in HDFS with partitioning
4. **Processing**: Spark jobs for data transformation and analytics
5. **Quality**: Data profiling and anomaly detection
6. **Output**: Processed data available for downstream consumption

## Performance Metrics
- **Throughput**: 100K+ records per minute
- **Latency**: End-to-end processing < 30 minutes
- **Reliability**: 99.9% data ingestion success rate
- **Storage**: Efficient compression achieving 70% space savings

## Learning Outcomes
- Real-world data engineering challenges
- API integration and rate limiting strategies  
- Large-scale data processing optimization
- Data quality and validation techniques
- Workflow orchestration and monitoring
- Performance tuning for big data systems