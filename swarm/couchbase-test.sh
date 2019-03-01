#!/bin/bash

# Start benchmark
docker stack deploy --compose-file docker-compose_mqtt.yml --compose-file docker-compose_worker.yml --compose-file docker-compose_coordinator.yml --compose-file docker-compose_couchbase.yml db-test
