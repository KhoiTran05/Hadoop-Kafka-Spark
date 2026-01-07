#!/bin/bash

set -e

echo "Starting Spark streaming job..."

if ! curl -s http://localhost:8080 > /dev/null; then
    echo "Spark master is not running. Please start the services first with docker-compose up -d"
    exit 1
fi

echo "Starting Weather streaming job..."
docker-compose exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --driver-memory 1g \
    --executor-memory 1g \
    --executor-cores 1 \
    --num-executors 2 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0,io.delta:delta-core_2.12:2.4.0,org.postgresql:postgresql:42.7.0 \
    //opt/bitnami/spark/streaming/weather_stream.py

echo "Waiting for streaming jobs to initialize..."
sleep 30

echo "Streaming jobs started successfully!"
echo ""
echo "Monitor jobs at:"
echo "  - Spark Master UI: http://localhost:8080"
echo "  - Spark Applications: http://localhost:8080/api/v1/applications"
echo ""