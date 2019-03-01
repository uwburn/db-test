"use strict";

const BaseRealTimeSmallMachine = require("../base/RealTimeSmallMachine");
const CassandraMachineSink = require("./CassandraMachineSink");

module.exports = class RealTimeSmallMachine extends BaseRealTimeSmallMachine {

  constructor(id, workerId, workloadOpts, databaseOpts, mqttClient) {
    super(id, workerId, workloadOpts, mqttClient);

    this.sink = new CassandraMachineSink(databaseOpts);
  }

};