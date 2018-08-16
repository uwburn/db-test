"use strict";

const RealTimeMachine = require(`./RealTimeMachine`);
const SmallMachineSampler = require(`./SmallMachineType`);

module.exports = class RealTimeSmallMachine extends RealTimeMachine {

    constructor(id, workerId, workloadOpts, mqttClient) {
        super(id, workerId, workloadOpts, new SmallMachineSampler(workloadOpts), mqttClient);
    }

};