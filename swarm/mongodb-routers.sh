#!/bin/bash

# Start mongodb instances
docker stack deploy --compose-file docker-compose_mongodb-routers.yml db-test
