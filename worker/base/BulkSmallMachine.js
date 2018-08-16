"use strict";

const BulkMachine = require(`./BulkMachine`);
const SmallMachineSampler = require(`./SmallMachineSampler`);

module.exports = class BulkSmallMachine extends BulkMachine {

    constructor(id, workerId, workloadOpts, mqttClient) {
        super(id, workerId, workloadOpts, new SmallMachineSampler(workloadOpts), mqttClient);
    }

};