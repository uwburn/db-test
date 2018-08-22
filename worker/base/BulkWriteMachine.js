"use strict";

const BaseWorkload = require(`./BaseWorkload`);
const MachineDataStreams = require(`./MachineStreams`);

const MAX_WORKER_DELAY = 10000;

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

        let bulkWritesSink = this.sink.recordStream();
        this.addWriteStream(bulkWritesSink);
        let bulkWritesSource = this.machineDataStreams.bulkWrites();
        this.addReadStream(bulkWritesSource);
        bulkWritesSource.stream.pipe(bulkWritesSink.stream);

        resolve();
      }, this.workerDelay);
    });
  }

};