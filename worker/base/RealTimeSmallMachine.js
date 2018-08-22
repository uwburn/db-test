"use strict";

const RealTimeMachine = require(`./RealTimeMachine`);
const Source = require(`./SmallMachineSource`);

module.exports = class RealTimeSmallMachine extends RealTimeMachine {

    constructor(id, workerId, workloadOpts, mqttClient) {
        super(id, workerId, workloadOpts, new Source(workloadOpts), mqttClient);
    }

};