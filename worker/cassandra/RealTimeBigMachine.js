"use strict";

const BaseRealTimeBigMachine = require(`../base/RealTimeBigMachine`);
const CassandraMachineInterface = require(`./CassandraMachineInterface`);

module.exports = class RealTimeBigMachine extends BaseRealTimeBigMachine {

  constructor(id, workerId, workloadOpts, databaseOpts, mqttClient) {
    super(id, workerId, workloadOpts, mqttClient);

    this.dbInterface = new CassandraMachineInterface(databaseOpts);
  }

};