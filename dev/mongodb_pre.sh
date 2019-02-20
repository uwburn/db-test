#!/bin/bash

docker-compose -f docker-compose.yml -f docker-compose_mongodb.yml up
docker-compose -f docker-compose.yml -f docker-compose_mongodb.yml down -v
