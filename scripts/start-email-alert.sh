#!/bin/bash

set -e 

echo "Starting email alert consumer ..."

if ! docker-compose ps kafka | grep -q "Up"; then
    echo "Kafka is not running. Please start services first."
    exit 1
fi

if ! docker-compose ps redis | grep -q "Up"; then
    echo "Redis is not running. Please start services first."
    exit 1
fi

if ! docker-compose ps jupyter | grep -q "Up"; then
    echo "Jupyter is not running. Starting Jupyter..."
    docker-compose up -d jupyter
    sleep 10
fi

echo "Starting email alert consumer in background..."
docker-compose exec -d jupyter python /home/jovyan/jobs/streaming/email_alert_consumer.py

echo "Email alert consumer started successfully!"
echo ""
echo "Monitor consumer:"
echo "  - View Jupyter logs: docker-compose logs -f jupyter"
echo ""
echo "To stop consumer:"
echo "  docker-compose exec jupyter pkill -f email_alert_consumer.py"