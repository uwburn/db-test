"use strict";

const BaseWorkload = require(`./BaseWorkload`);
const MachineDataStreams = require(`./MachineStreams`);

module.exports = class RealTimeMachine extends BaseWorkload {

  constructor(id, workerId, workloadOpts, machineType, mqttClient) {
    super(id, workerId, mqttClient);
    this.workloadOpts = workloadOpts;

    this.machineDataStreams = new MachineDataStreams(workloadOpts, machineType);
  }

  async setupStreams() {
    let recordStream = this.dbInterface.recordStream();
    this.addWriteStream(recordStream);
    let realTimeWritesStream = this.machineDataStreams.realTimeWrites();
    this.addReadStream(realTimeWritesStream);
    realTimeWritesStream.stream.pipe(recordStream.stream);

    let queryStream = this.dbInterface.queryStream();
    this.addWriteStream(queryStream);
    let realTimeReadsStream = this.machineDataStreams.realTimeReads();
    this.addReadStream(realTimeReadsStream);
    realTimeReadsStream.stream.pipe(queryStream.stream);
  }

};