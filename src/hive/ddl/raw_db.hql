CREATE DATABASE IF NOT EXISTS bronze_db
COMMENT 'Original data, untouched source format'
LOCATION 'hdfs://namenode:9000/warehouse/bronze_db';

USE bronze_db;

CREATE EXTERNAL TABLE IF NOT EXISTS weather_data (
    city STRING,
    country STRING,
    weather_main STRING,
    weather_description STRING,
    temperature DOUBLE,
    pressure INT,
    humidity INT,
    visibility INT,
    wind_speed DOUBLE,
    record_timestamp STRING
) PARTITIONED BY (
    year STRING,
    month STRING
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION '/raw-data/weather/';

CREATE EXTERNAL TABLE IF NOT EXISTS financial_data (
    symbol STRING,
    txn_date STRING,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume INT,
    record_timestamp STRING
) PARTITIONED BY (
    year STRING,
    month STRING
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
LOCATION '/raw-data/stock/';


