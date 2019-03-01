"use strict";

const BaseBulkReadSmallMachine = require("../base/BulkReadSmallMachine");
const CassandraMachineSink = require("./CassandraMachineSink");

module.exports = class BulkSmallMachine extends BaseBulkReadSmallMachine {

  constructor(id, workerId, workloadOpts, databaseOpts, mqttClient) {
    super(id, workerId, workloadOpts, mqttClient);

    this.sink = new CassandraMachineSink(databaseOpts);
  }

};