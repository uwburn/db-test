"use strict";

const BaseBulkWriteBigMachine = require(`../base/BulkWriteBigMachine`);
const CassandraMachineInterface = require(`./CassandraMachineInterface`);

module.exports = class BulkBigMachine extends BaseBulkWriteBigMachine {

  constructor(id, workerId, workloadOpts, databaseOpts, mqttClient) {
    super(id, workerId, workloadOpts, mqttClient);

    this.dbInterface = new CassandraMachineInterface(databaseOpts);
  }

};