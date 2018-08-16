"use strict";

const BaseRealTimeSmallMachine = require(`../base/RealTimeSmallMachine`);
const MongoMachineInterface = require(`./MongoMachineInterface`);

module.exports = class RealTimeSmallMachine extends BaseRealTimeSmallMachine {

  constructor(id, workerId, workloadOpts, databaseOpts, mqttClient) {
    super(id, workerId, workloadOpts, mqttClient);

    this.dbInterface = new MongoMachineInterface(databaseOpts);
  }

  getDbInterface() {
    return this.dbInterface;
  }

};