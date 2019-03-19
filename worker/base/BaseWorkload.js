"use strict";

const LOG_INTERVAL = 10000;

module.exports = class BaseWorkload {

  constructor(id, workerId, mqttClient) {
    this.id = id;
    this.workerId = workerId;
    this.mqttClient = mqttClient;

    this.readStreams = [];
    this.writeSinks = [];
  }

  async sleep(duration) {
    await new Promise((resolve) => {
      setTimeout(resolve, duration);
    });
  }

  addReadStream(stream) {
    this.readStreams.push(stream);
  }

  removeReadStream(stream) {
    let index = this.readStreams.indexOf(stream);
    if (index > -1)
      this.readStreams.splice(index, 1);
  }

  addWriteSink(sink) {
    this.writeSinks.push(sink);
  }

  removeWriteSinks(sink) {
    let index = this.writeSinks.indexOf(sink);
    if (index > -1)
      this.writeSinks.splice(index, 1);
  }

  async sinksCleared() {
    await Promise.all(this.writeSinks.map((s) => s.promise));

    this.endTime = new Date().getTime();

    this.log();
    console.log("Workload completed");
  }

  log() {
    this.mqttClient.publish(`worker/${this.workerId}/work/${this.id}/log`, JSON.stringify(this.stats()));
  }

  stats() {
    let percents = this.readStreams.map(e => e.percent());
    let percent = percents.reduce((acc, e, i, arr) => {
      return acc + e / arr.length;
    }, 0);

    let stats = this.writeSinks.reduce((acc, e) => {
      acc.totalReads += e.reads;
      acc.totalReadLatency += e.totalReadLatency;
      acc.totalSuccessfulReads += e.successfulReads;
      acc.totalReadRows += e.readRows;
      acc.totalWrites += e.writes;
      acc.totalWriteLatency += e.totalWriteLatency;
      acc.totalSuccessfulWrites += e.successfulWrites;
      acc.totalErrors += e.errors;

      return acc;
    }, {
      totalReads: 0,
      totalReadLatency: 0,
      totalSuccessfulReads: 0,
      totalReadRows: 0,
      totalWrites: 0,
      totalWriteLatency: 0,
      totalSuccessfulWrites: 0,
      totalErrors: 0
    });


    let avgReadLatency = stats.totalReadLatency / stats.totalSuccessfulReads;
    if (isNaN(avgReadLatency))
      avgReadLatency = null;

    let avgWriteLatency = stats.totalWriteLatency / stats.totalSuccessfulWrites;
    if (isNaN(avgWriteLatency))
      avgWriteLatency = null;

    return {
      time: new Date().getTime(),
      startTime: this.startTime,
      endTime: this.endTime,
      reads: stats.totalReads,
      readLatency: avgReadLatency,
      totalReadRows: stats.totalReadRows,
      writes: stats.totalWrites,
      writeLatency: avgWriteLatency,
      errors: stats.totalErrors,
      percent: percent
    };
  }

  async run() {
    await this.init();

    let logInterval = setInterval(this.log.bind(this), LOG_INTERVAL);

    this.startTime = new Date().getTime();

    await this.train();

    await this.setupStreams();

    await this.sinksCleared();

    clearInterval(logInterval);

    await this.cleanup();
  }

  async init() {
    await this.sink.init();
  }

  async setupStreams() {
    throw new Error("Base class doesn't implement setupStreams method");
  }

  async cleanup() {
    await this.sink.cleanup();
  }

  async train() {
    let now = new Date();
    let nilUuid = "00000000-0000-0000-0000-000000000000";

    this.startTime = now.getTime();

    let source = this.machineDataStreams.source;
    for (let k in source.sampleMethods)
      await this.sink.train(k, source.sampleTypes[k], source.sampleIntervals[k], source.sampleMethods[k](nilUuid, now));
  }

};