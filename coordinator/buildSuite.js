"use strict";

const uuidv4 = require("uuid/v4");
const moment = require("moment");
const MongoClient = require("mongodb").MongoClient;
const cassandra = require("cassandra-driver");
const couchbase = require("couchbase");
const bluebird = require("bluebird");
const request = require("request-promise-native");

const BULK_READS_LIMIT = 32768;

module.exports = function (config) {
  let suite;
  switch (config.suite) {
  case "MachineData":
    suite = buildMachineDataSuite(config.database, config.databaseOpts, config.suiteOpts);
    break;
  default:
    throw new Error(`Unknown suite ${suite}`);
  }

  suite.comment = config.comment;

  return suite;
};

function buildMachineDataSuite(database, databaseOpts, suiteOptions) {
  let id = uuidv4();
  let startYear = moment(suiteOptions.startYear, "YYYY");
  suiteOptions.machines = parseInt(suiteOptions.machines);
  suiteOptions.machineUptime = parseFloat(suiteOptions.machineUptime);
  let machineSize = suiteOptions.machineSize || "Big";
  let machineTypeId = uuidv4();

  const machines = [];
  for (let i = 0; i < suiteOptions.machines; ++i)
    machines.push(uuidv4());

  return {
    id: id,
    description: machineSize + " machines",
    database: database,
    machines: machines,
    get length() {
      return suiteOptions.years * 3;
    },
    getStepName(stepIndex) {
      let year = Math.floor(stepIndex / 3);

      let mod = stepIndex % 3;
      switch (mod) {
      case 0:
        return `Year ${year + 1} - Bulk writes`;
      case 1:
        return `Year ${year + 1} - Bulk reads`;
      case 2:
        return `Year ${year + 1} - Real time`;
      }
    },
    getStepTags(stepIndex) {
      let mod = stepIndex % 3;
      switch (mod) {
      case 0:
        return ["WRITE_SPEED"];
      case 1:
        return ["READ_SPEED"];
      case 2:
        return ["WRITE_LATENCY", "READ_LATENCY"];
      }
    },
    getStepForWorker(stepIndex, workerIndex, totalWorkers) {
      let year = Math.floor(stepIndex / 3);
      startYear.clone().add(year, "year").valueOf();

      let machinesPerWorker = Math.floor(machines.length / totalWorkers);

      let startIndex = machinesPerWorker * workerIndex;
      let endIndex = machinesPerWorker * (workerIndex + 1);
      if ((totalWorkers - 1) === workerIndex)
        endIndex = undefined;

      let mod = stepIndex % 3;
      switch (mod) {
      case 0:
        return {
          name: `Year ${year + 1} - Bulk writes`,
          type: "Bulk writes",
          database: database,
          databaseOpts: databaseOpts,
          workload: `BulkWrite${machineSize}Machine`,
          workloadOpts: {
            startTime: startYear.clone().add(year, "year").valueOf(),
            endTime: startYear.clone().add(year, "year").endOf("year").valueOf(),
            machineUptime: suiteOptions.machineUptime,
            machines: machines.slice(startIndex, endIndex),
            machineTypeId: machineTypeId
          },
          tags: ["WRITE_SPEED"]
        };
      case 1:
        return {
          name: `Year ${year + 1} - Bulk reads`,
          type: "Bulk reads",
          database: database,
          databaseOpts: databaseOpts,
          workload: `BulkRead${machineSize}Machine`,
          workloadOpts: {
            startTime: startYear.clone().add(year, "year").valueOf(),
            endTime: startYear.clone().add(year, "year").endOf("year").valueOf(),
            machineUptime: suiteOptions.machineUptime,
            machines: machines.slice(startIndex, endIndex),
            machineTypeId: machineTypeId,
            bulkReadsLimit: Math.floor(BULK_READS_LIMIT / totalWorkers)
          },
          tags: ["READ_SPEED"]
        };
      case 2:
        return {
          name: `Year ${year + 1} - Real time`,
          type: "Real time",
          database: database,
          databaseOpts: databaseOpts,
          workload: `RealTime${machineSize}Machine`,
          workloadOpts: {
            startTime: startYear.clone().add(year, "year").add(6, "month").subtract(450, "seconds").valueOf(),
            duration: 900000,
            machineUptime: suiteOptions.machineUptime,
            machines: machines.slice(startIndex, endIndex),
            machineTypeId: machineTypeId
          },
          tags: ["WRITE_LATENCY", "READ_LATENCY"]
        };
      }
    },
    async prepareDatabase() {
      switch (database) {
      case "mongo":
        return await prepareMachineDataMongo(databaseOpts);
      case "cassandra":
        return await prepareMachineDataCassandra(databaseOpts);
      case "couchbase":
        return await prepareMachineDataCouchbase(databaseOpts);
      }
    }
  };

}

async function prepareMachineDataMongo(databaseOpts) {
  console.log("Waiting for MongoDB");
  let mongoClient;
  // eslint-disable-next-line
  while (true) {
    try {
      mongoClient = await MongoClient.connect(databaseOpts.url, databaseOpts.options);
      break;
    }
    catch (err) { /* eslint-disable-line */ }
  }

  mongoClient.close();
}

async function prepareMachineDataCassandra(databaseOpts) {
  let cassandraClient = new cassandra.Client(databaseOpts);

  console.log("Waiting for Cassandra");
  // eslint-disable-next-line
  while (true) {
    try {
      await cassandraClient.execute("SELECT * FROM system_schema.keyspaces", [], {});
      break;
    }
    catch (err) { /* eslint-disable-line */ }
  }

  let replicationFactor = databaseOpts.replicationFactor || 1;

  console.log("Preparing keyspace and tables");
  await cassandraClient.execute("DROP KEYSPACE IF EXISTS db_test;", [], {});
  await cassandraClient.execute(`CREATE KEYSPACE db_test WITH replication = {'class' : 'SimpleStrategy', 'replication_factor' : ${replicationFactor}};`, [], {});
  await cassandraClient.execute("USE db_test;", [], {});
  await cassandraClient.execute("CREATE TABLE time_complex (dt uuid, g text, d uuid, p timestamp, t timestamp, v blob, PRIMARY KEY (( dt, g, d, p ), t)) WITH COMPACTION = {'class': 'LeveledCompactionStrategy'} AND COMPACT STORAGE;", [], {});
  await cassandraClient.execute("CREATE TABLE interval_closed (dt uuid, g text, d uuid, p timestamp, b timestamp, id uuid, v blob, PRIMARY KEY ((dt, g, d, p), b, id)) WITH COMPACTION = {'class': 'LeveledCompactionStrategy'} AND COMPACT STORAGE;", [], {});
  await cassandraClient.execute("CREATE TABLE interval_open (dt uuid, g text, d uuid, st timestamp, id uuid, v blob, PRIMARY KEY ((dt, g, d), st, id)) WITH COMPACTION = {'class': 'LeveledCompactionStrategy'} AND COMPACT STORAGE;", [], {});

  await cassandraClient.shutdown();
}

async function prepareMachineDataCouchbase(databaseOpts) {
  console.log("Waiting for Couchbase");

  if (databaseOpts.setupCluster) {
    // eslint-disable-next-line
    while (true) {
      try {
        await request(databaseOpts.httpUrl);
        break;
      }
      catch (err) { /* eslint-disable-line */ }
    }

    await new Promise((resolve) => setTimeout(resolve, 5000));

    console.log("Setup Couchbase cluster");

    await request({
      method: "POST",
      url: databaseOpts.httpUrl + "/settings/web?just_validate=1",
      form: {
        username: databaseOpts.username,
        password: databaseOpts.password,
        port: "SAME"
      }
    });

    await request({
      method: "POST",
      url: databaseOpts.httpUrl + "/settings/stats",
      form: {
        sendStats: "false"
      }
    });

    await request({
      method: "POST",
      url: databaseOpts.httpUrl + "/node/controller/setupServices",
      form: {
        services: "kv,index,fts,n1ql",
        setDefaultMemQuotas: "true"
      }
    });

    await request({
      method: "POST",
      url: databaseOpts.httpUrl + "/settings/indexes",
      form: {
        storageMode: "forestdb"
      }
    });

    await request({
      method: "POST",
      url: databaseOpts.httpUrl + "/pools/default",
      form: {
        clusterName: "db-test"
      }
    });

    await request({
      method: "POST",
      url: databaseOpts.httpUrl + "/settings/web?just_validate=0",
      form: {
        username: databaseOpts.username,
        password: databaseOpts.password,
        port: "SAME"
      }
    });

    console.log("Waiting for services to settle");

    await new Promise((resolve) => setTimeout(resolve, 15000));
  }

  let couchbaseCluster = new couchbase.Cluster(databaseOpts.url);
  bluebird.promisifyAll(couchbaseCluster);
  couchbaseCluster.authenticate(databaseOpts.username, databaseOpts.password);

  let couchbaseManager = couchbaseCluster.manager(databaseOpts.username, databaseOpts.password);
  bluebird.promisifyAll(couchbaseManager);

  try {
    await couchbaseManager.removeBucketAsync("db-test");
  }
  catch(err) { /* eslint-disable-line */ }
  
  await couchbaseManager.createBucketAsync("db-test", {
    flushEnabled: 1,
    ramQuotaMB: databaseOpts.bucketRamQuotaMB || 1024,
    replicaNumber: databaseOpts.replicaNumber || 3
  });

  await new Promise((resolve) => setTimeout(resolve, 5000));

  let couchbaseBucket = await new Promise((resolve) => {
    let bucket = couchbaseCluster.openBucket("db-test");
    bluebird.promisifyAll(bucket);
    bucket.once("connect", () => resolve(bucket));
    bucket.on("error", (err) => console.log(err));
  });

  await new Promise((resolve) => setTimeout(resolve, 5000));

  let couchbaseBucketManager = couchbaseBucket.manager();
  bluebird.promisifyAll(couchbaseBucketManager);

  await couchbaseBucketManager.upsertDesignDocumentAsync("time_complex", {
    views: {
      by_group_device_time: {
        map : "function (doc, meta) { if (doc.type !== 'time_complex') return; emit([doc.group, doc.device, doc.time], { time: doc.time, value: doc.value }); }"
      }
    }
  });

  let q = couchbase.N1qlQuery.fromString("CREATE INDEX `db-test_interval_device_times` ON `db-test`(`type`, `group`, `device`, `startTime`, `endTime`) WHERE type = 'interval' USING GSI;");
  await couchbaseBucket.queryAsync(q);

}