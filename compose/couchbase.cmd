docker-compose -f docker-compose.yml -f docker-compose_couchbase.yml up --build --abort-on-container-exit --scale worker=2
docker-compose -f docker-compose.yml -f docker-compose_couchbase.yml down -v