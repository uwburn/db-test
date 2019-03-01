"use strict";

const BaseBulkReadBigMachine = require("../base/BulkReadBigMachine");
const CassandraMachineSink = require("./CassandraMachineSink");

module.exports = class BulkBigMachine extends BaseBulkReadBigMachine {

  constructor(id, workerId, workloadOpts, databaseOpts, mqttClient) {
    super(id, workerId, workloadOpts, mqttClient);

    this.sink = new CassandraMachineSink(databaseOpts);
  }

};