"use strict";

const BaseBulkWriteSmallMachine = require(`../base/BulkWriteSmallMachine`);
const CassandraMachineInterface = require(`./CassandraMachineInterface`);

module.exports = class BulkSmallMachine extends BaseBulkWriteSmallMachine {

  constructor(id, workerId, workloadOpts, databaseOpts, mqttClient) {
    super(id, workerId, workloadOpts, mqttClient);

    this.dbInterface = new CassandraMachineInterface(databaseOpts);
  }

};