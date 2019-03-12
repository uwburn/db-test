#!/bin/bash

# Start cassandra seed
docker stack deploy --compose-file docker-compose_cassandra-seed.yml db-test

# Start cassandra node 1
sleep 45
docker stack deploy --compose-file docker-compose_cassandra-node1.yml db-test

# Start cassandra node 2
sleep 45
docker stack deploy --compose-file docker-compose_cassandra-node2.yml db-test

# Start cassandra node 3
sleep 45
docker stack deploy --compose-file docker-compose_cassandra-node3.yml db-test

# Start cassandra node 4
sleep 45
docker stack deploy --compose-file docker-compose_cassandra-node4.yml db-test

# Start cassandra node 5
sleep 45
docker stack deploy --compose-file docker-compose_cassandra-node5.yml db-test

# Start cassandra node 6
sleep 45
docker stack deploy --compose-file docker-compose_cassandra-node6.yml db-test

# Start cassandra node 7
sleep 45
docker stack deploy --compose-file docker-compose_cassandra-node7.yml db-test

# Start cassandra node 8
sleep 45
docker stack deploy --compose-file docker-compose_cassandra-node8.yml db-test
