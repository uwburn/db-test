"use strict";

const uuidv4 = require(`uuid/v4`);
const moment = require(`moment`);
const MongoClient = require(`mongodb`).MongoClient;
const cassandra = require(`cassandra-driver`);

module.exports = function (database, databaseOpts, suite, suiteOptions) {
  switch (suite) {
    case `MachineData`:
      return buildMachineDataSuite(database, databaseOpts, suiteOptions);
    default:
      throw new Error(`Unknown suite`);
  }
};

function buildMachineDataSuite(database, databaseOpts, suiteOptions) {
  let startYear = moment(suiteOptions.startYear, `YYYY`);
  suiteOptions.machines = parseInt(suiteOptions.machines);
  suiteOptions.machineUptime = parseFloat(suiteOptions.machineUptime);
  let machineSize = suiteOptions.machineSize || `Big`;
  let machineTypeId = uuidv4();

  const machines = [];
  for (let i = 0; i < suiteOptions.machines; ++i)
    machines.push(uuidv4());

  return {
    machines: machines,
    get length() {
      return suiteOptions.years;
    },
    getStepName(stepIndex) {
      let year = Math.floor(stepIndex / 2);

      let mod = stepIndex % 2;
      switch (mod) {
        case 0:
          return `Year ${year + 1} - Bulk insert`;
        case 1:
          return `Year ${year + 1} - Real time`;
      }
    },
    getStepForWorker(stepIndex, workerIndex, totalWorkers) {
      let year = Math.floor(stepIndex / 2);
      startYear.clone().add(year, `year`).valueOf();

      let machinesPerWorker = Math.floor(machines.length / totalWorkers);

      let startIndex = machinesPerWorker * workerIndex;
      let endIndex = machinesPerWorker * (workerIndex + 1);
      if ((totalWorkers - 1) === workerIndex)
        endIndex = undefined;

      let mod = stepIndex % 2;
      switch (mod) {
        case 0:
          return {
            name: `Year ${year + 1} - Bulk insert`,
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
            name: `Year ${year + 1} - Real time`,
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

  console.log(`Preparing db, collections and indexes`);
  let db = mongoClient.db(`db-test`);
  let timeComplexColl = db.collection(`timeComplex`);
  let intervalColl = db.collection(`interval`);

  await timeComplexColl.ensureIndex({ "_id.device": 1 });
  await timeComplexColl.ensureIndex({ "_id.time": 1 });

  await intervalColl.ensureIndex({ startTime: 1 });
  await intervalColl.ensureIndex({ endTime: 1 });

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

  console.log(`Preparing keyspace and tables`);
  await cassandraClient.execute(`CREATE KEYSPACE IF NOT EXISTS db_test WITH replication = {'class' : 'SimpleStrategy', 'replication_factor' : 1};`, [], {});
  await cassandraClient.execute(`USE db_test;`, [], {});
  await cassandraClient.execute(`CREATE TABLE IF NOT EXISTS time_complex (device_type text, device text, group text, timestamp timestamp, original_timestamp timestamp, value text, PRIMARY KEY (( device_type, device, group ), timestamp));`, [], {});
  await cassandraClient.execute(`CREATE TABLE IF NOT EXISTS time_flat_complex (device_type text, device text, group text, path text, timestamp timestamp, original_timestamp timestamp, value text, PRIMARY KEY (( device_type, device, group ), path, timestamp));`, [], {});
  await cassandraClient.execute(`CREATE TABLE IF NOT EXISTS interval (device_type text, device text, group text, start_time timestamp, end_time timestamp, value text, PRIMARY KEY (device_type, device, group, start_time, end_time));`, [], {});

  await cassandraClient.shutdown();
}