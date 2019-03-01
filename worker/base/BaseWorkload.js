"use strict";

const LOG_INTERVAL = 10000;

module.exports = class BaseWorkload {

  constructor(id, workerId, mqttClient) {
    this.id = id;
    this.workerId = workerId;
    this.mqttClient = mqttClient;

    this.readStreams = [];
    this.writeStreams = [];
  }

  addReadStream(stream) {
    this.readStreams.push(stream);
  }

  removeReadStream(stream) {
    let index = this.readStreams.indexOf(stream);
    if (index > -1)
      this.readStreams.splice(index, 1);
  }

  addWriteStream(stream) {
    this.writeStreams.push(stream);
  }

  removeWriteStream(stream) {
    let index = this.writeStreams.indexOf(stream);
    if (index > -1)
      this.writeStreams.splice(index, 1);
  }

  get streams() {
    return this.readStreams.concat(this.writeStreams);
  }

  removeStream(stream) {
    let index = this.readStreams.indexOf(stream);
    if (index > -1)
      this.readStreams.splice(index, 1);

    index = this.writeStreams.indexOf(stream);
    if (index > -1)
      this.writeStreams.splice(index, 1);
  }

  async streamsCleared() {
    await new Promise((resolve) => {
      this.writeStreams.forEach((e) => {
        e.stream.once("finish", () => {
          e.finished = true;

          if (this.writeStreams.filter(e => !e.finished).length > 0)
            return;

          this.endTime = new Date().getTime();

          this.log();
          console.log("Workload completed");

          resolve();
        });
      });
    });
  }

  log() {
    this.mqttClient.publish(`worker/${this.workerId}/work/${this.id}/log`, JSON.stringify(this.stats()));
  }

  stats() {
    let percents = this.readStreams.map(e => e.percent());
    let percent = percents.reduce((acc, e, i, arr) => {
      return acc + e / arr.length;
    }, 0);

    let stats = this.writeStreams.reduce((acc, e) => {
      acc.totalReads += e.reads;
      acc.totalReadLatency += e.totalReadLatency;
      acc.totalSuccessfulReads += e.successfulReads;
      acc.totalWrites += e.writes;
      acc.totalWriteLatency += e.totalWriteLatency;
      acc.totalSuccessfulWrites += e.successfulWrites;
      acc.totalErrors += e.errors;

      return acc;
    }, {
      totalReads: 0,
      totalReadLatency: 0,
      totalSuccessfulReads: 0,
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

    await this.setupStreams();

    await this.streamsCleared();

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

};