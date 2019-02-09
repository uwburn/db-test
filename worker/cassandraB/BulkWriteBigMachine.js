"use strict";

const BaseBulkWriteBigMachine = require(`../base/BulkWriteBigMachine`);
const CassandraMachineSink = require(`./CassandraMachineSink`);

module.exports = class BulkBigMachine extends BaseBulkWriteBigMachine {

  constructor(id, workerId, workloadOpts, databaseOpts, mqttClient) {
    super(id, workerId, workloadOpts, mqttClient);

    this.sink = new CassandraMachineSink(databaseOpts);
  }

};