"use strict";

const BaseWorkload = require("./BaseWorkload");
const MachineDataStreams = require("./MachineStreams");

const MAX_WORKER_DELAY = 5000;

module.exports = class BulkWriteMachine extends BaseWorkload {

  constructor(id, workerId, workloadOpts, source, mqttClient) {
    super(id, workerId, mqttClient);

    this.workerDelay = Math.round(Math.random() * MAX_WORKER_DELAY);

    this.machineDataStreams = new MachineDataStreams(workloadOpts, source);
  }

  async setupStreams() {
    console.log(`Waiting worker delay (${Math.round(this.workerDelay / 1000)} s) to pass... `);

    await this.sleep(this.workerDelay);

    this.startTime = new Date().getTime();
    let bulkWritesSource = this.machineDataStreams.bulkWrites();
    this.addReadStream(bulkWritesSource);
    bulkWritesSource.stream.pause();
    let bulkWritesSink = this.sink.recordSink(bulkWritesSource.stream);
    this.addWriteSink(bulkWritesSink);
    bulkWritesSource.stream.resume();
  }

};