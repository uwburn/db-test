"use strict";

const BaseBulkBigMachine = require(`../base/BulkBigMachine`);
const MongoMachineInterface = require(`./MongoMachineInterface`);

module.exports = class BulkBigMachine extends BaseBulkBigMachine {

  constructor(id, workerId, workloadOpts, databaseOpts, mqttClient) {
    super(id, workerId, workloadOpts, mqttClient);

    this.dbInterface = new MongoMachineInterface(databaseOpts);
  }

  getDbInterface() {
    return this.dbInterface;
  }

}