"use strict";

const uuidv4 = require(`uuid/v4`);
const moment = require(`moment`);
const MongoClient = require(`mongodb`).MongoClient;
const cassandra = require(`cassandra-driver`);
const couchbase = require('couchbase');
const bluebird = require("bluebird");
const request = require("request-promise-native");

const BULK_READS_LIMIT = 100000;

module.exports = function (database, databaseOpts, suite, suiteOptions) {
  switch (suite) {
    case `MachineData`:
      return buildMachineDataSuite(database, databaseOpts, suiteOptions);
    default:
      throw new Error(`Unknown suite ${suite}`);
  }
};

function buildMachineDataSuite(database, databaseOpts, suiteOptions) {
  let id = uuidv4();
  let startYear = moment(suiteOptions.startYear, `YYYY`);
  suiteOptions.machines = parseInt(suiteOptions.machines);
  suiteOptions.machineUptime = parseFloat(suiteOptions.machineUptime);
  let machineSize = suiteOptions.machineSize || `Big`;
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
    getStepForWorker(stepIndex, workerIndex, totalWorkers) {
      let year = Math.floor(stepIndex / 3);
      startYear.clone().add(year, `year`).valueOf();

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
            type: `Bulk writes`,
            database: database,
            databaseOpts: databaseOpts,
            workload: `BulkWrite${machineSize}Machine`,
            workloadOpts: {
              startTime: startYear.clone().add(year, `year`).valueOf(),
              endTime: startYear.clone().add(year, `year`).month(11).date(31).hour(23).minute(45).valueOf(),
              machineUptime: suiteOptions.machineUptime,
              machines: machines.slice(startIndex, endIndex),
              machineTypeId: machineTypeId
            }
          };
        case 1:
          return {
            name: `Year ${year + 1} - Bulk reads`,
            type: `Bulk reads`,
            database: database,
            databaseOpts: databaseOpts,
            workload: `BulkRead${machineSize}Machine`,
            workloadOpts: {
              startTime: startYear.clone().add(year, `year`).valueOf(),
              endTime: startYear.clone().add(year, `year`).month(11).date(31).hour(23).minute(45).valueOf(),
              machineUptime: suiteOptions.machineUptime,
              machines: machines.slice(startIndex, endIndex),
              machineTypeId: machineTypeId,
              bulkReadsLimit: Math.floor(BULK_READS_LIMIT / totalWorkers)
            }
          };
        case 2:
          return {
            name: `Year ${year + 1} - Real time`,
            type: `Real time`,
            database: database,
            databaseOpts: databaseOpts,
            workload: `RealTime${machineSize}Machine`,
            workloadOpts: {
              startTime: startYear.clone().add(year, `year`).month(11).date(31).hour(23).minute(45).valueOf(),
              duration: 900000,
              machineUptime: suiteOptions.machineUptime,
              machines: machines.slice(startIndex, endIndex),
              machineTypeId: machineTypeId
            }
          };
      }
    },
    async prepareDatabase() {
      switch (database) {
        case `mongo`:
          return await prepareMachineDataMongo(databaseOpts);
        case `cassandra`:
          return await prepareMachineDataCassandra(databaseOpts);
        case "couchbase":
          return await prepareMachineDataCouchbase(databaseOpts);
      }
    }
  };

}

async function prepareMachineDataMongo(databaseOpts) {
  console.log(`Waiting for MongoDB`);
  let mongoClient;
  while (true) {
    try {
      mongoClient = await MongoClient.connect(databaseOpts.url, databaseOpts.options);
      break;
    } catch (err) { }
  }

  mongoClient.close();
}

async function prepareMachineDataCassandra(databaseOpts) {
  let cassandraClient = new cassandra.Client(databaseOpts);

  console.log(`Waiting for Cassandra`);
  while (true) {
    try {
      await cassandraClient.execute(`SELECT * FROM system_schema.keyspaces`, [], {});
      break;
    } catch (err) { }
  }

  let replicationFactor = databaseOpts.replicationFactor || 1;

  console.log(`Preparing keyspace and tables`);
  await cassandraClient.execute(`CREATE KEYSPACE IF NOT EXISTS db_test WITH replication = {'class' : 'SimpleStrategy', 'replication_factor' : ${replicationFactor}};`, [], {});
  await cassandraClient.execute(`USE db_test;`, [], {});
  await cassandraClient.execute(`CREATE TABLE IF NOT EXISTS time_complex (device_type text, group text, device text, timestamp timestamp, original_timestamp timestamp, value text, PRIMARY KEY (( device_type, group, device ), timestamp));`, [], {});
  await cassandraClient.execute(`CREATE TABLE IF NOT EXISTS interval (device_type text, group text, device text, start_time timestamp, end_time timestamp, value text, PRIMARY KEY (device_type, group, device, start_time, end_time));`, [], {});

  await cassandraClient.shutdown();
}

async function prepareMachineDataCouchbase(databaseOpts) {
  console.log(`Waiting for Couchbase`);

  if (databaseOpts.setupCluster) {
    while (true) {
      try {
        await request(databaseOpts.httpUrl);
        break;
      }
      catch (err) { }
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

    console.log("Waiting for services to settle")

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
  catch(err) { }
  
  await couchbaseManager.createBucketAsync("db-test", {
    flushEnabled: 1,
    ramQuotaMB: databaseOpts.bucketRamQuotaMB || 1024
  });

  await new Promise((resolve) => setTimeout(resolve, 5000));

  let couchbaseBucket = await new Promise((resolve, reject) => {
    let bucket = couchbaseCluster.openBucket("db-test");
    bluebird.promisifyAll(bucket);
    bucket.once("connect", () => resolve(bucket));
    bucket.on("error", (err) => console.log(err));
  });

  await new Promise((resolve) => setTimeout(resolve, 5000));

  let q = couchbase.N1qlQuery.fromString('CREATE PRIMARY INDEX `db-test_primary` ON `db-test`;');
  await couchbaseBucket.queryAsync(q);

  q = couchbase.N1qlQuery.fromString("CREATE INDEX `db-test_interval_device_times` ON `db-test`(`type`, `group`, `device`, `startTime`, `endTime`) WHERE type = 'interval' USING GSI;");
  await couchbaseBucket.queryAsync(q);

  q = couchbase.N1qlQuery.fromString("CREATE INDEX `db-test_time_complex_device_time` ON `db-test`(`type`, `group`, `device`, `time`) WHERE type = 'time_complex' USING GSI;");
  await couchbaseBucket.queryAsync(q);
}