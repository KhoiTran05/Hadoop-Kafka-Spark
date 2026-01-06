#!/bin/bash

set -e

if ! docker-compose ps kafka | grep -q "Up"; then
    echo "Kafka is not running. Start the service first"
    exit 1
fi

if ! docker-compose ps jupyter | grep -q "Up"; then
    echo "Jupyter is not running. Starting jupyter ..."
    docker-compose up -d jupyter 
    echo "Waiting for jupyter to be ready ..."
    sleep 10
fi

echo "Starting continuous data ingestion in background ..."
docker-compose exec -d jupyter python //home/jovyan/jobs/ingestion/api/streaming.py

echo "Data ingestion started successfully!"
echo ""
echo "Monitor ingestion:"
echo "  - Check Kafka topics and messages on Kafka UI: http://localhost:8085"
echo "  - View Jupyter logs: docker-compose logs -f jupyter"
echo ""
echo "To stop ingestion:"
echo "  docker-compose exec jupyter pkill -f streaming.py"
