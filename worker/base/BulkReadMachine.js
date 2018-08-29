"use strict";

const BaseWorkload = require(`./BaseWorkload`);
const MachineDataStreams = require(`./MachineStreams`);

const MAX_WORKER_DELAY = 5000;

module.exports = class BulkMachine extends BaseWorkload {

  constructor(id, workerId, workloadOpts, source, mqttClient) {
    super(id, workerId, mqttClient);

    this.workerDelay = Math.round(Math.random() * MAX_WORKER_DELAY);

    this.machineDataStreams = new MachineDataStreams(workloadOpts, source);
  }

  async setupStreams() {
    await new Promise((resolve) => {
      console.log(`Waiting worker delay (${Math.round(this.workerDelay / 1000)} s) to pass... `);

      setTimeout(() => {
        this.startTime = new Date().getTime();

        let bulkReadsSink = this.sink.queryStream();
        this.addWriteStream(bulkReadsSink);
        let bulkReadsSource = this.machineDataStreams.bulkReads();
        this.addReadStream(bulkReadsSource);
        bulkReadsSource.stream.pipe(bulkReadsSink.stream);

        resolve();
      }, this.workerDelay);
    });
  }

};