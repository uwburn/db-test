#!/bin/bash

# Start Couchbase instances
docker stack deploy --compose-file docker-compose_couchbase-cluster.yml db-test
