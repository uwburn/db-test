"use strict";

const RealTimeMachine = require("./RealTimeMachine");
const Source = require("./MidMachineSource");

module.exports = class RealTimeMidMachine extends RealTimeMachine {

  constructor(id, workerId, workloadOpts, mqttClient) {
    super(id, workerId, workloadOpts, new Source(workloadOpts), mqttClient);
  }

};