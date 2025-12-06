#!/bin/sh

echo 'Building services'
sleep 2
docker compose build

echo 'Launching services | Press Ctrl+c when done reading the logs.'
sleep 5

echo 'Launching Kafka and Redis in Background as their logs are not relevant.'
sleep 2
docker compose up -d redis kafka

echo 'Launching remaining services.'
sleep 2
docker compose up api emqx mqtt-grpc-bridge car station