"use strict";

const RealTimeMachine = require(`./RealTimeMachine`);
const BigMachineSampler = require(`./BigMachineType`);

module.exports = class RealTimeBigMachine extends RealTimeMachine {

    constructor(id, workerId, workloadOpts, mqttClient) {
        super(id, workerId, workloadOpts, new BigMachineSampler(workloadOpts), mqttClient);
    }

};