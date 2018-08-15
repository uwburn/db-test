"use strict";

const BaseRealTimeSmallMachine = require(`../base/RealTimeSmallMachine`);
const CassandraMachineInterface = require(`./CassandraMachineInterface`);

module.exports = class RealTimeSmallMachine extends BaseRealTimeSmallMachine {

  constructor(id, workerId, workloadOpts, databaseOpts, mqttClient) {
    super(id, workerId, workloadOpts, mqttClient);

    this.dbInterface = new CassandraMachineInterface(databaseOpts);
  }

  getDbInterface() {
    return this.dbInterface;
  }

}