#!/bin/bash

docker-compose -f docker-compose_db-test.yml -f docker-compose_mongodb.yml up --build --abort-on-container-exit
docker-compose -f docker-compose_db-test.yml -f docker-compose_mongodb.yml down -v