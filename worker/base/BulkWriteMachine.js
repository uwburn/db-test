"use strict";

const BaseWorkload = require(`./BaseWorkload`);
const MachineDataStreams = require(`./MachineStreams`);

const MAX_WORKER_DELAY = 10000;

module.exports = class BulkMachine extends BaseWorkload {

  constructor(id, workerId, workloadOpts, machineType, mqttClient) {
    super(id, workerId, mqttClient);

    this.workerDelay = Math.round(Math.random() * MAX_WORKER_DELAY);

    this.machineDataStreams = new MachineDataStreams(workloadOpts, machineType);
  }

  getStats() {
    let percent = Math.round(this.getDbInterface().writes / this.machineDataStreams.totalSamples * 100);
    if (isNaN(percent))
      percent = 100;

    let avgReadLatency = this.getDbInterface().totalReadLatency / this.getDbInterface().successfulReads;
    if (isNaN(avgReadLatency))
      avgReadLatency = null;

    let avgWriteLatency = this.getDbInterface().totalWriteLatency / this.getDbInterface().successfulWrites;
    if (isNaN(avgWriteLatency))
      avgWriteLatency = null;

    return {
      time: new Date().getTime(),
      startTime: this.startTime,
      endTime: this.endTime,
      reads: this.getDbInterface().reads,
      readLatency: avgReadLatency,
      writes: this.getDbInterface().writes,
      writeLatency: avgWriteLatency,
      errors: this.getDbInterface().errors,
      percent: percent
    };
  }

  localLog(stats) {
    console.log(`Writes: ${stats.writes}, errors: ${stats.errors}, ${stats.percent}%`);
  }

  async _run() {
    await new Promise((resolve) => {
      console.log(`Waiting worker delay (${Math.round(this.workerDelay / 1000)} s) to pass... `);

      setTimeout(() => {
        this.startTime = new Date().getTime();

        let recordStream = this.getDbInterface().recordStream();
        this.machineDataStreams.bulkStream().pipe(recordStream);

        recordStream.once("finish", () => {
          this.endTime = new Date().getTime();

          this.log();
          console.log(`Workload completed`);

          resolve();
        });
      }, this.workerDelay);
    });
  }

};