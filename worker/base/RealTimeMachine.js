"use strict";

const BaseWorkload = require(`./BaseWorkload`);
const MachineDataStreams = require(`./MachineStreams`);

module.exports = class RealTimeMachine extends BaseWorkload {

  constructor(id, workerId, workloadOpts, machineType, mqttClient) {
    super(id, workerId, mqttClient);
    this.workloadOpts = workloadOpts;

    this.machineDataStreams = new MachineDataStreams(workloadOpts, machineType);
  }

  stats() {
    let percent = Math.round((this.machineDataStreams.absTime - this.workloadOpts.startTime) / this.workloadOpts.duration * 100);
    if (isNaN(percent))
      percent = 100;

    let avgReadLatency = this.dbInterface.totalReadLatency / this.dbInterface.successfulReads;
    if (isNaN(avgReadLatency))
      avgReadLatency = null;

    let avgWriteLatency = this.dbInterface.totalWriteLatency / this.dbInterface.successfulWrites;
    if (isNaN(avgWriteLatency))
      avgWriteLatency = null;

    return {
      time: new Date().getTime(),
      startTime: this.startTime,
      endTime: this.endTime,
      reads: this.dbInterface.reads,
      readLatency: avgReadLatency,
      writes: this.dbInterface.writes,
      writeLatency: avgWriteLatency,
      errors: this.dbInterface.errors,
      percent: percent
    };
  }

  async _run() {
    await new Promise((resolve) => {
      this.startTime = new Date().getTime();

      let recordStream = this.dbInterface.recordStream();
      this.machineDataStreams.realTimeWrites().pipe(recordStream);

      recordStream.once("finish", () => {
        this.endTime = new Date().getTime();

        this.log();
        console.log(`Workload completed`);

        resolve();
      });
    });
  }

};