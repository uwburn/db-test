"use strict";

const RealTimeMachine = require("./RealTimeMachine");
const Source = require("./BigMachineSource");

module.exports = class RealTimeBigMachine extends RealTimeMachine {

  constructor(id, workerId, workloadOpts, mqttClient) {
    super(id, workerId, workloadOpts, new Source(workloadOpts), mqttClient);
  }

};