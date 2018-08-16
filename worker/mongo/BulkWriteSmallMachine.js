"use strict";

const BaseBulkWriteSmallMachine = require(`../base/BulkWriteSmallMachine`);
const MongoMachineInterface = require(`./MongoMachineInterface`);

module.exports = class BulkSmallMachine extends BaseBulkWriteSmallMachine {

  constructor(id, workerId, workloadOpts, databaseOpts, mqttClient) {
    super(id, workerId, workloadOpts, mqttClient);

    this.dbInterface = new MongoMachineInterface(databaseOpts);
  }

  getDbInterface() {
    return this.dbInterface;
  }

};