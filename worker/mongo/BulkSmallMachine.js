"use strict";

const BaseBulkSmallMachine = require(`../base/BulkSmallMachine`);
const MongoMachineInterface = require(`./MongoMachineInterface`);

module.exports = class BulkSmallMachine extends BaseBulkSmallMachine {

  constructor(id, workerId, workloadOpts, databaseOpts, mqttClient) {
    super(id, workerId, workloadOpts, mqttClient);

    this.dbInterface = new MongoMachineInterface(databaseOpts);
  }

  getDbInterface() {
    return this.dbInterface;
  }

}