#!/bin/bash

docker-compose up -d

cd worker
npm start > /dev/null &
npm start > /dev/null &
npm start > /dev/null &
npm start > /dev/null &
cd ..

cd coordinator
SUITE=machines_10years_mongodb.json npm start