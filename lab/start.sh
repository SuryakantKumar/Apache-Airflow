#!/bin/bash

# Build the Base Images based on the Dockerfiles then Startup all the containers at once

docker build -t hadoop-base docker/hadoop/hadoop-base && \
docker build -t hive-base docker/hive/hive-base && \
docker build -t spark-base docker/spark/spark-base && \
docker-compose up -d --build
