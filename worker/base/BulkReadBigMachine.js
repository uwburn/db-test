"use strict";

const BulkReadMachine = require(`./BulkReadMachine`);
const Source = require(`./BigMachineSource`);

module.exports = class BulkBigMachine extends BulkReadMachine {

  constructor(id, workerId, workloadOpts, mqttClient) {
    super(id, workerId, workloadOpts, new Source(workloadOpts), mqttClient);
  }

};