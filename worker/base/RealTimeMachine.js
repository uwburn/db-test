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
    let realTimeWritesSource = this.machineDataStreams.realTimeWrites();
    this.addReadStream(realTimeWritesSource);
    realTimeWritesSource.stream.pause();
    let realTimeWritesSink = this.sink.recordSink(realTimeWritesSource.stream);
    this.addWriteSink(realTimeWritesSink);

    let realTimeQueriesSource = this.machineDataStreams.realTimeReads();
    this.addReadStream(realTimeQueriesSource);
    realTimeQueriesSource.stream.pause();
    let realTimeQueriesSink = this.sink.querySink(realTimeQueriesSource.stream);
    this.addWriteSink(realTimeQueriesSink);

    realTimeWritesSource.stream.resume();
    realTimeQueriesSource.stream.resume();
  }

};