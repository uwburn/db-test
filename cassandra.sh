#!/bin/bash

docker-compose -f docker-compose.yml -f docker-compose_cassandra.yml up --build --abort-on-container-exit --scale worker=12
docker-compose -f docker-compose.yml -f docker-compose_cassandra.yml down -v
