# DB Test
Suite to test database performance and deployment with Docker. Currently operates tests for time-series, but might be extended to other cases if needed.

# Setup

## MongoDB
MongoDB requires setting up the replica-set for three and six nodes Swarm setup, while sharding setup is required for nine nodes Swarm setup.
The configuration has not been automated, meaning the containers will be started but will not configure the replica-set and sharding by themselves. To do so, Mongo Shell needs to be accessed from the containers by running `docker exec -it <conainer-name> mongo --port <mongo-port>`

### Three nodes replica set
Execute the following in one of the container Mongo Shell:
```
rs.initiate( {
  _id : "rs0",
  members: [
    { _id: 0, host: "mongodb1:27017" },
    { _id: 1, host: "mongodb2:27017" },
    { _id: 2, host: "mongodb3:27017" }
  ]
});
```

### Six nodes replica set
Execute the following in one of the container Mongo Shell:
```
rs.initiate( {
  _id : "rs0",
  members: [
    { _id: 0, host: "mongodb1:27017" },
    { _id: 1, host: "mongodb2:27017" },
    { _id: 2, host: "mongodb3:27017" },
    { _id: 3, host: "mongodb4:27017" },
    { _id: 4, host: "mongodb5:27017" },
    { _id: 5, host: "mongodb6:27017" }
  ]
});
```

### Nine nodes sharding
This setup requires twelve instances across nine Swarm nodes: two replica sets (one primary + two secondaries) filling up six nodes, and a replica set (one primary + two secondaries) plus three routers filling up the remainder three nodes.

Please note that ports are changed when running clusterin mode:
* Configuration servers: 27019
* Sharded servers: 27018
* Routers: 27017

Start up the replica-sets containers.

Execute the following in one of the configuration server container Mongo Shell:
```
rs.initiate( {
  _id : "cfg",
  members: [
    { _id: 0, host: "mongodb-cfg-1:27019" },
    { _id: 1, host: "mongodb-cfg-2:27019" },
    { _id: 2, host: "mongodb-cfg-3:27019" }
  ]
});
```

Execute the following in one of the container Mongo Shell for the first shard:
```
rs.initiate( {
  _id : "rs0",
  members: [
    { _id: 0, host: "mongodb-rs0-1:27018" },
    { _id: 1, host: "mongodb-rs0-2:27018" },
    { _id: 2, host: "mongodb-rs0-3:27018" }
  ]
});
```

Execute the following in one of the container Mongo Shell for the second shard:
```
rs.initiate( {
  _id : "rs1",
  members: [
    { _id: 0, host: "mongodb-rs1-1:27018" },
    { _id: 1, host: "mongodb-rs1-2:27018" },
    { _id: 2, host: "mongodb-rs1-3:27018" }
  ]
});
```

Start up the routers.

Execute the following in one of the router server container Mongo Shell:
```
sh.addShard("rs0/mongodb-rs0-1:27018,mongodb-rs0-2:27018,mongodb-rs0-3:27018");
sh.addShard("rs1/mongodb-rs1-1:27018,mongodb-rs1-2:27018,mongodb-rs1-3:27018");

sh.enableSharding("db-test");
use db-test;
```

Then according to the case being tested, run:
* Small
```
db.createCollection("alarm_interval");
db.createCollection("counters_time_complex");
db.createCollection("geo_time_complex");
db.createCollection("setup_time_complex");
sh.shardCollection("db-test.alarm_interval", { device: "hashed" });
sh.shardCollection("db-test.counters_time_complex", { "_id.device": "hashed" });
sh.shardCollection("db-test.geo_time_complex", { "_id.device": "hashed" });
sh.shardCollection("db-test.setup_time_complex", { "_id.device": "hashed" });
```
* Mid
```
db.createCollection("status_time_complex");
db.createCollection("state_interval");
db.createCollection("counters_time_complex");
db.createCollection("setup_time_complex");
db.createCollection("geo_time_complex");
db.createCollection("alarm_interval");
sh.shardCollection("db-test.status_time_complex", { "_id.device": "hashed" });
sh.shardCollection("db-test.state_interval", { device: "hashed" });
sh.shardCollection("db-test.counters_time_complex", { "_id.device": "hashed" });
sh.shardCollection("db-test.setup_time_complex", { "_id.device": "hashed" });
sh.shardCollection("db-test.geo_time_complex", { "_id.device": "hashed" });
sh.shardCollection("db-test.alarm_interval", { device: "hashed" });
```
* Large
```
db.createCollection("state_interval");
db.createCollection("status_time_complex");
db.createCollection("counters_time_complex");
db.createCollection("setup_interval");
db.createCollection("cooling_time_complex");
db.createCollection("extrusion_time_complex");
db.createCollection("motion_time_complex");
db.createCollection("alarm_interval");
sh.shardCollection("db-test.state_interval", { device: "hashed" });
sh.shardCollection("db-test.status_time_complex", { "_id.device": "hashed" });
sh.shardCollection("db-test.counters_time_complex", { "_id.device": "hashed" });
sh.shardCollection("db-test.setup_interval", { device: "hashed" });
sh.shardCollection("db-test.cooling_time_complex", { "_id.device": "hashed" });
sh.shardCollection("db-test.extrusion_time_complex", { "_id.device": "hashed" });
sh.shardCollection("db-test.motion_time_complex", { "_id.device": "hashed" });
sh.shardCollection("db-test.alarm_interval", { device: "hashed" });
```

## Cassandra
WIP

## Couchbase
WIP
