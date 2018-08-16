"use strict";

const BaseBulkBigMachine = require(`../base/BulkBigMachine`);
const CassandraMachineInterface = require(`./CassandraMachineInterface`);

module.exports = class BulkBigMachine extends BaseBulkBigMachine {

  constructor(id, workerId, workloadOpts, databaseOpts, mqttClient) {
    super(id, workerId, workloadOpts, mqttClient);

    this.dbInterface = new CassandraMachineInterface(databaseOpts);
  }

  getDbInterface() {
    return this.dbInterface;
  }

};