#!/bin/bash

docker-compose -f docker-compose.yml -f docker-compose_couchbase.yml up --build --abort-on-container-exit --scale worker=8
docker-compose -f docker-compose.yml -f docker-compose_couchbase.yml down -v