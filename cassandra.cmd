docker-compose -f docker-compose.yml -f docker-compose_cassandra.yml up --build --abort-on-container-exit --scale worker=4
docker-compose -f docker-compose.yml -f docker-compose_cassandra.yml down -v