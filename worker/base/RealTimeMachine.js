"use strict";

const BaseWorkload = require("./BaseWorkload");
const MachineDataStreams = require("./MachineStreams");

module.exports = class RealTimeMachine extends BaseWorkload {

  constructor(id, workerId, workloadOpts, source, mqttClient) {
    super(id, workerId, mqttClient);
    this.workloadOpts = workloadOpts;

    this.machineDataStreams = new MachineDataStreams(workloadOpts, source);
  }

  async setupStreams() {
    let realTimeWritesSink = this.sink.recordStream();
    this.addWriteStream(realTimeWritesSink);
    let realTimeWritesSource = this.machineDataStreams.realTimeWrites();
    this.addReadStream(realTimeWritesSource);
    realTimeWritesSource.stream.pipe(realTimeWritesSink.stream);

    let realTimeQueriesSink = this.sink.queryStream();
    this.addWriteStream(realTimeQueriesSink);
    let realTimeQueriesSource = this.machineDataStreams.realTimeReads();
    this.addReadStream(realTimeQueriesSource);
    realTimeQueriesSource.stream.pipe(realTimeQueriesSink.stream);
  }

};