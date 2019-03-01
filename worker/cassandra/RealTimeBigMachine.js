"use strict";

const BaseRealTimeBigMachine = require("../base/RealTimeBigMachine");
const CassandraMachineSink = require("./CassandraMachineSink");

module.exports = class RealTimeBigMachine extends BaseRealTimeBigMachine {

  constructor(id, workerId, workloadOpts, databaseOpts, mqttClient) {
    super(id, workerId, workloadOpts, mqttClient);

    this.sink = new CassandraMachineSink(databaseOpts);
  }

};