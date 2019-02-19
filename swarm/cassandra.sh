#!/bin/bash

# Start cassandra seed
docker stack deploy --compose-file docker-compose_cassandra-seed.yml db-test

# Start cassandra node 1
sleep 60
docker stack deploy --compose-file docker-compose_cassandra-node.yml db-test

# Start cassandra node 2
sleep 60 
docker service scale db-test_cassandra-node=2

# Start benchmark
docker stack deploy --compose-file docker-compose_mqtt.yml --compose-file docker-compose_worker.yml --compose-file docker-compose_coordinator.yml --compose-file docker-compose_cassandra.yml db-test