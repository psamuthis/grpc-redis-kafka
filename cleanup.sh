#!/bin/sh

echo 'Shutting down services.'
sleep 2
docker compose down -v

echo 'Cleaning up docker'
sleep 2
docker system prune -f