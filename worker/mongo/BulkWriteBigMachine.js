"use strict";

const BaseBulkWriteBigMachine = require(`../base/BulkWriteBigMachine`);
const MongoMachineInterface = require(`./MongoMachineInterface`);

module.exports = class BulkBigMachine extends BaseBulkWriteBigMachine {

  constructor(id, workerId, workloadOpts, databaseOpts, mqttClient) {
    super(id, workerId, workloadOpts, mqttClient);

    this.dbInterface = new MongoMachineInterface(databaseOpts);
  }

  getDbInterface() {
    return this.dbInterface;
  }

};