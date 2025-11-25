#!/bin/bash
# Load ingested data to HDFS
set -e

EXECUTION_DATE=$1

echo "==== Setting up HDFS directories ===="

WEATHER_LOCAL_FILE="/data/raw/weather/weather_${EXECUTION_DATE}.json"
STOCK_LOCAL_FILE="/data/raw/stock/stock_${EXECUTION_DATE}.json"

WEATHER_HDFS_FILE="/raw-data/weather/year=${EXECUTION_DATE:0:4}/month=${EXECUTION_DATE:5:2}/${EXECUTION_DATE}.json"
STOCK_HDFS_FILE="/raw-data/stock/year=${EXECUTION_DATE:0:4}/month=${EXECUTION_DATE:5:2}/${EXECUTION_DATE}.json"

hdfs dfs -mkdir -p $(dirname "${WEATHER_HDFS_FILE}")
hdfs dfs -mkdir -p $(dirname "${STOCK_HDFS_FILE}")

echo "==== Uploading data to HDFS ===="

hdfs dfs -put -f "${WEATHER_LOCAL_FILE}" "${WEATHER_HDFS_FILE}"
hdfs dfs -put -f "${STOCK_LOCAL_FILE}" "${STOCK_HDFS_FILE}"

echo "==== Data uploaded to HDFS successfully ===="

echo "==== Verifying data upload ===="

hdfs dfs -ls /raw-data/

echo "==== Total data size ===="

hdfs dfs -du -s -h /raw-data/