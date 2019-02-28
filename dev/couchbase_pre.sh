#!/bin/bash

docker-compose -f docker-compose.yml -f docker-compose_couchbase.yml up
#docker-compose -f docker-compose.yml -f docker-compose_couchbase.yml down -v