#!/bin/bash

# Start cassandra seed
docker stack deploy --compose-file docker-compose_cassandra-seed.yml db-test

# Start cassandra node 1
sleep 60
docker stack deploy --compose-file docker-compose_cassandra-node1.yml db-test

# Start cassandra node 2
sleep 60
docker stack deploy --compose-file docker-compose_cassandra-node2.yml db-test
