"use strict";

const BulkReadMachine = require("./BulkReadMachine");
const Source = require("./SmallMachineSource");

module.exports = class BulkReadSmallMachine extends BulkReadMachine {

  constructor(id, workerId, workloadOpts, mqttClient) {
    super(id, workerId, workloadOpts, new Source(workloadOpts), mqttClient);
  }

};