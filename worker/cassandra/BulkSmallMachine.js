"use strict";

const BaseBulkSmallMachine = require(`../base/BulkSmallMachine`);
const CassandraMachineInterface = require(`./CassandraMachineInterface`);

module.exports = class BulkSmallMachine extends BaseBulkSmallMachine {

  constructor(id, workerId, workloadOpts, databaseOpts, mqttClient) {
    super(id, workerId, workloadOpts, mqttClient);

    this.dbInterface = new CassandraMachineInterface(databaseOpts);
  }

  getDbInterface() {
    return this.dbInterface;
  }

};