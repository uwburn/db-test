"use strict";

const uuidv4 = require(`uuid/v4`);
const moment = require(`moment`);

module.exports = function(database, databaseOpts, suite, suiteOptions) {
  switch (suite) {
    case "MachineData":
      return buildMachineDataSuite(database, databaseOpts, suiteOptions);
    default:
      throw new Error("Unknown suite");
  }
};

function buildMachineDataSuite(database, databaseOpts, suiteOptions) {
  let startYear = moment(suiteOptions.startYear, "YYYY");
  suiteOptions.machines = parseInt(suiteOptions.machines);
  suiteOptions.machineUptime = parseFloat(suiteOptions.machineUptime);

  const machines = [];
  for (let i = 0; i < suiteOptions.machines; ++i)
    machines.push(uuidv4());

  return {
    machines: machines,
    get length() {
      return suiteOptions.years;
    },
    getStepName(stepIndex) {
      let year = stepIndex;

      return "Year " + (year + 1) + " - Bulk insert";
    },
    getStepForWorker(stepIndex, workerIndex, totalWorkers) {
      let year = stepIndex;
      startYear.clone().add(year, "year").valueOf();

      let machinesPerWorker = Math.floor(machines.length / totalWorkers);

      let startIndex = machinesPerWorker * workerIndex;
      let endIndex = machinesPerWorker * (workerIndex + 1);
      if ((totalWorkers - 1) === workerIndex)
        endIndex = undefined;

      return {
        name: "Year " + (year + 1) + " - Bulk insert",
        database: database,
        databaseOpts: databaseOpts,
        workload: "BulkMachineData",
        workloadOpts: {
          startTime: startYear.clone().add(year, "year").valueOf(),
          endTime: startYear.clone().add(year, "year").month(11).date(31).hour(23).minute(45).valueOf(),
          machineUptime: suiteOptions.machineUptime,
          machines: machines.slice(startIndex, endIndex)
        }
      }
    }
  };

}