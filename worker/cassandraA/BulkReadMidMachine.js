"use strict";

const BaseBulkReadMidMachine = require(`../base/BulkReadMidMachine`);
const CassandraMachineSink = require(`./CassandraMachineSink`);

module.exports = class BulkBigMachine extends BaseBulkReadMidMachine {

  constructor(id, workerId, workloadOpts, databaseOpts, mqttClient) {
    super(id, workerId, workloadOpts, mqttClient);

    this.sink = new CassandraMachineSink(databaseOpts);
  }

};