"use strict";

const BaseBulkWriteSmallMachine = require("../base/BulkWriteSmallMachine");
const CassandraMachineSink = require("./CassandraMachineSink");

module.exports = class BulkSmallMachine extends BaseBulkWriteSmallMachine {

  constructor(id, workerId, workloadOpts, databaseOpts, mqttClient) {
    super(id, workerId, workloadOpts, mqttClient);

    this.sink = new CassandraMachineSink(databaseOpts);
  }

};