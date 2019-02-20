"use strict";

const BulkWriteMachine = require(`./BulkWriteMachine`);
const Source = require(`./BigMachineSource`);

module.exports = class BulkWriteMidMachine extends BulkWriteMachine {

  constructor(id, workerId, workloadOpts, mqttClient) {
    super(id, workerId, workloadOpts, new Source(workloadOpts), mqttClient);
  }

};