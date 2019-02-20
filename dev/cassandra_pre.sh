#!/bin/bash

docker-compose -f docker-compose.yml -f docker-compose_cassandra.yml up
docker-compose -f docker-compose.yml -f docker-compose_cassandra.yml down -v