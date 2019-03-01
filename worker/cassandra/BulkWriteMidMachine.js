"use strict";

const BaseBulkWriteMidMachine = require("../base/BulkWriteMidMachine");
const CassandraMachineSink = require("./CassandraMachineSink");

module.exports = class BulkBigMachine extends BaseBulkWriteMidMachine {

  constructor(id, workerId, workloadOpts, databaseOpts, mqttClient) {
    super(id, workerId, workloadOpts, mqttClient);

    this.sink = new CassandraMachineSink(databaseOpts);
  }

};