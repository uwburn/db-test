"use strict";

const BulkWriteMachine = require(`./BulkWriteMachine`);
const SmallMachineSampler = require(`./SmallMachineType`);

module.exports = class BulkSmallMachine extends BulkWriteMachine {

    constructor(id, workerId, workloadOpts, mqttClient) {
        super(id, workerId, workloadOpts, new SmallMachineSampler(workloadOpts), mqttClient);
    }

};