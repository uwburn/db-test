"use strict";

const BaseRealTimeBigMachine = require(`../base/RealTimeBigMachine`);
const MongoMachineInterface = require(`./MongoMachineInterface`);

module.exports = class RealTimeBigMachine extends BaseRealTimeBigMachine {

  constructor(id, workerId, workloadOpts, databaseOpts, mqttClient) {
    super(id, workerId, workloadOpts, mqttClient);

    this.dbInterface = new MongoMachineInterface(databaseOpts);
  }

  getDbInterface() {
    return this.dbInterface;
  }

};