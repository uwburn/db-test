"use strict";

const BulkReadMachine = require(`./BulkReadMachine`);
const Source = require(`./MidMachineSource`);

module.exports = class BulkReadMidMachine extends BulkReadMachine {

  constructor(id, workerId, workloadOpts, mqttClient) {
    super(id, workerId, workloadOpts, new Source(workloadOpts), mqttClient);
  }

};