"use strict";

const BulkWriteMachine = require(`./BulkWriteMachine`);
const BigMachineSampler = require(`./BigMachineType`);

module.exports = class BulkBigMachine extends BulkWriteMachine {

  constructor(id, workerId, workloadOpts, mqttClient) {
    super(id, workerId, workloadOpts, new BigMachineSampler(workloadOpts), mqttClient);
  }

};