"use strict";

const BulkMachine = require(`./BulkMachine`);
const BigMachineSampler = require(`./BigMachineSampler`);

module.exports = class BulkBigMachine extends BulkMachine {

  constructor(id, workerId, workloadOpts, mqttClient) {
    super(id, workerId, workloadOpts, new BigMachineSampler(workloadOpts), mqttClient);
  }

}