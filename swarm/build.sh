#!/bin/bash

docker-compose -f docker-compose_build.yml build
docker-compose -f docker-compose_build.yml push