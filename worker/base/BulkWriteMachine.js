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

  async setupStreams() {
    await new Promise((resolve) => {
      console.log(`Waiting worker delay (${Math.round(this.workerDelay / 1000)} s) to pass... `);

      setTimeout(() => {
        this.startTime = new Date().getTime();

        let recordStream = this.dbInterface.recordStream();
        this.addWriteStream(recordStream);
        let bulkWritesReadStream = this.machineDataStreams.bulkWrites();
        this.addReadStream(bulkWritesReadStream);
        bulkWritesReadStream.stream.pipe(recordStream.stream);

        resolve();
      }, this.workerDelay);
    });
  }

};