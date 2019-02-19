#!/bin/bash

docker-compose -f docker-compose_registry.yml up
docker-compose -f docker-compose_build.yml --build push

docker-compose -f docker-compose_mqtt.yml -f docker-compose_cassandra-seed.yml -f docker-compose_coordinator.yml -f docker-compose_worker.yml up --abort-on-container-exit --scale worker=8
docker-compose -f docker-compose_mqtt.yml -f docker-compose_cassandra-seed.yml -f docker-compose_coordinator.yml -f docker-compose_worker.yml down -v
