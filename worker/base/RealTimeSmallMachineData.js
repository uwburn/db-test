"use strict";

const SmallMachineData = require(`./SmallMachineData`);

const MAX_WORKER_DELAY = 10000;

const WRITE_INTERVALS = {
  counters: 3600000,
  setup: 86400000,
  mng: 86400000,
  geo: 86400000,
  alarm: 3888000000
};

const TIME_STEP = Math.min(
  WRITE_INTERVALS.counters,
  WRITE_INTERVALS.setup,
  WRITE_INTERVALS.mng,
  WRITE_INTERVALS.geo,
  WRITE_INTERVALS.alarm,
  1000
);

let MAX_STEP = Math.max(
  WRITE_INTERVALS.counters,
  WRITE_INTERVALS.setup,
  WRITE_INTERVALS.mng,
  WRITE_INTERVALS.geo,
  WRITE_INTERVALS.alarm
);

const MAX_RETRIES = 5;

const LOG_INTERVAL = 10000;

module.exports = class RealTimeSmallMachineData extends SmallMachineData {

  constructor(id, workerId, workloadOpts, databaseOpts, mqttClient) {
    super(workloadOpts);
    this.id = id;
    this.workerId = workerId;
    this.databaseOpts = databaseOpts;
    this.mqttClient = mqttClient;

    this.workerDelay = Math.round(Math.random() * MAX_WORKER_DELAY);

    let activeMachines = Math.round(this.workloadOpts.machines.length * this.workloadOpts.machineUptime);
    this.machines = {};
    for (let i = 0; i < activeMachines; ++i) {
      let machineId = this.workloadOpts.machines[i];
      let machinePhase = MAX_STEP * Math.random();
      machinePhase = Math.round(machinePhase / TIME_STEP) * TIME_STEP;

      this.machines[machineId] = {
        machinePhase: machinePhase
      }
    }

    this.absTime = this.workloadOpts.startTime;
    this.absDate = new Date(this.absTime);

    this.reads = 0;
    this.writes = 0;
    this.errors = 0;
    this.successfulWrites = 0;
    this.totalWriteLatency = 0;
  }

  log() {
    let percent = Math.round((this.absTime - this.workloadOpts.startTime) / this.workloadOpts.duration * 100);
    if (isNaN(percent))
      percent = 100;

    let avgWriteLatency = this.totalWriteLatency/this.successfulWrites;
    if (isNaN(avgWriteLatency))
      avgWriteLatency = 0;

    console.log(`Writes: ${this.writes}, avg. write latency: ${Math.round(avgWriteLatency)} errors: ${this.errors}, ${percent}%`);

    this.mqttClient.publish(`worker/${this.workerId}/work/${this.id}/log`, JSON.stringify({
      time: new Date().getTime(),
      startTime: this.startTime,
      endTime: this.endTime,
      totalSamples: this.totalSamples,
      reads: this.reads,
      writes: this.writes,
      writeLatency: avgWriteLatency,
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

  async _run() {
    await new Promise((resolve) => {
      this.startTime = new Date().getTime();

      let logInterval = setInterval(this.log.bind(this), LOG_INTERVAL);

      let pending = 0;
      let finished = false;
      let checkPending = () => {
        if (finished && pending === 0) {
          this.endTime = new Date().getTime();
          clearInterval(logInterval);

          this.log();
          console.log(`Workload completed`);

          resolve();
        }
      }

      let record = (machineId, groupName, absDate) => {
        setImmediate(async () => {
          ++pending;
          let retry = 0;
          ++this.writes;
          let writeStartTime = new Date().getTime();
          while (++retry <= MAX_RETRIES) {
            try {
              let sample = this.sample(machineId, groupName, absDate);
              await this.record(machineId, groupName, sample);
              ++this.successfulWrites;
              this.totalWriteLatency += new Date().getTime() - writeStartTime;
            } catch(err) {
              if (retry === MAX_RETRIES) {
                console.error(err);
                ++this.errors;
              }
            }
          }
          --pending;
          checkPending();
        });
      }

      let realTimeInterval = setInterval(() => {
        for (let machineId in this.machines) {
          let machinePhase = this.machines[machineId].machinePhase;
          for (let groupName in WRITE_INTERVALS) {
            if ((this.absTime + machinePhase) % WRITE_INTERVALS[groupName] === 0)
              record(machineId, groupName, this.absDate);
          }
        }

        this.absTime += TIME_STEP;
        this.absDate = new Date(this.absTime);
      }, 1000);

    
      setTimeout(() => {
        finished = true;
        clearInterval(realTimeInterval);
        checkPending();
      }, this.workloadOpts.duration);
    });
  }

  async record(id, groupName, sample) {
    throw new Error("Base class doesn't implement record method");
  }

};
