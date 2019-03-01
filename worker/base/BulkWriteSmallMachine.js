"use strict";

const BulkWriteMachine = require("./BulkWriteMachine");
const Source = require("./SmallMachineSource");

module.exports = class BulkWriteSmallMachine extends BulkWriteMachine {

  constructor(id, workerId, workloadOpts, mqttClient) {
    super(id, workerId, workloadOpts, new Source(workloadOpts), mqttClient);
  }

};