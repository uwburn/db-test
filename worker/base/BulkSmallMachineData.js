"use strict";

const { Readable, Writable } = require('stream');

const SmallMachineData = require(`./SmallMachineData`);

const MAX_WORKER_DELAY = 10000;

const INTERVALS = {
  counters: 3600000,
  setup: 86400000,
  mng: 86400000,
  geo: 86400000,
  alarm: 3888000000
};

const TIME_STEP = Math.min(
  INTERVALS.counters,
  INTERVALS.setup,
  INTERVALS.mng,
  INTERVALS.geo,
  INTERVALS.alarm
);

const LOG_INTERVAL = 10000;

module.exports = class BulkSmallMachineData extends SmallMachineData {

  constructor(id, workerId, workloadOpts, databaseOpts, mqttClient) {
    super(workloadOpts);
    this.id = id;
    this.workerId = workerId;
    this.databaseOpts = databaseOpts;
    this.mqttClient = mqttClient;

    this.workerDelay = Math.round(Math.random() * MAX_WORKER_DELAY);

    let timeInterval = this.workloadOpts.endTime - this.workloadOpts.startTime;

    this.samples = {
      counters: Math.floor(timeInterval / INTERVALS.counters * this.workloadOpts.machineUptime),
      setup: Math.floor(timeInterval / INTERVALS.setup * this.workloadOpts.machineUptime),
      mng: Math.floor(timeInterval / INTERVALS.mng * this.workloadOpts.machineUptime),
      geo: Math.floor(timeInterval / INTERVALS.geo * this.workloadOpts.machineUptime),
      alarm: Math.floor(timeInterval / INTERVALS.alarm * this.workloadOpts.machineUptime)
    };

    this.totalSamples = 0;
    for (let k in this.samples) {
      this.totalSamples += this.samples[k];
    }
    this.totalSamples *= this.workloadOpts.machines.length;

    this.machines = {};
    for (let machineId of this.workloadOpts.machines) {
      let machineDelay = timeInterval * (1 - this.workloadOpts.machineUptime) * Math.random();
      machineDelay = Math.round(machineDelay / TIME_STEP) * TIME_STEP;

      this.machines[machineId] = {
        groups: {
          counters: 0,
          setup: 0,
          mng: 0,
          geo: 0,
          alarm: 0
        },
        machineDelay: machineDelay
      }
    }

    this.reads = 0;
    this.writes = 0;
    this.errors = 0;
  }

  log() {
    let percent = Math.round(this.writes / this.totalSamples * 100);
    if (isNaN(percent))
      percent = 100;

    console.log(`Writes: ${this.writes}/${this.totalSamples}, errors: ${this.errors}, ${percent}%`);

    this.mqttClient.publish(`worker/${this.workerId}/work/${this.id}/log`, JSON.stringify({
      time: new Date().getTime(),
      startTime: this.startTime,
      endTime: this.endTime,
      totalSamples: this.totalSamples,
      reads: this.reads,
      writes: this.writes,
      errors: this.errors,
      percent: percent
    }));
  }

  async run() {
    await this.init();
    await this._run();
    await this.cleanup();
  }

  async init() {
    throw new Error("Base class doesn't implement init method");
  }

  async cleanup() {
    throw new Error("Base class doesn't implement cleanup method");
  }

  get streamHighWatermark() {
    return 256;
  }

  machineDataStream() {
    let done = false;
    let absTime = this.workloadOpts.startTime;
    let absDate = new Date(absTime);
    let relTime = 0;
    let i = 0;
    let j = 0;

    let rs = Readable({
      objectMode: true,
      highWaterMark: this.streamHighWatermark
    });
    rs._read = (size) => {
      let readSamples = 0;
      while (!done) {
        done = true;

        let ids = Object.keys(this.machines);
        for (; i < ids.length; ++i) {
          let id = ids[i];
          let machine = this.machines[id];

          let groupNames = Object.keys(machine.groups);
          for (; j < groupNames.length; ++j) {
            let groupName = groupNames[j];

            done = false;
            if ((relTime + machine.machineDelay) % INTERVALS[groupName] === 0) {
              rs.push({
                id: id,
                groupName: groupName,
                sample: this.sample(id, groupName, absDate)
              });

              ++machine.groups[groupName];

              if (machine.groups[groupName] >= this.samples[groupName])
                delete machine.groups[groupName];

              if (++readSamples >= size)
                return;
            }
          }

          j = 0;
        }

        i = 0;
        absTime += TIME_STEP;
        absDate = new Date(absTime);
        relTime += TIME_STEP;
      }

      rs.push(null);
    };

    return rs;
  }

  recordStream() {
    let ws = Writable({
      objectMode: true,
      highWaterMark: this.streamHighWatermark
    });

    ws._write = (chunk, enc, callback) => {
      this.record(chunk.id, chunk.groupName, chunk.sample).catch((err) => {
        console.error(err);
        ++this.errors;
      }).then(() => {
        ++this.writes;
      }).then(callback);
    };

    return ws;
  }

  async _run() {
    await new Promise((resolve) => {
      console.log(`Waiting worker delay (${Math.round(this.workerDelay / 1000)} s) to pass... `);

      setTimeout(() => {
        let logInterval = setInterval(this.log.bind(this), LOG_INTERVAL);

        this.startTime = new Date().getTime();

        let recordStream = this.recordStream();
        this.machineDataStream().pipe(recordStream);

        recordStream.once("finish", () => {
          this.endTime = new Date().getTime();
          clearInterval(logInterval);

          this.log();
          console.log(`Workload completed`);

          resolve();
        });
      }, this.workerDelay);
    });
  }

  async record(id, groupName, sample) {
    throw new Error("Base class doesn't implement record method");
  }

};
