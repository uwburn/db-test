"use strict";

const { Readable } = require('stream');

const HIGH_WATERMARK = 256;
const REAL_TIME_STEP = 1000;

module.exports = class MachineStreams {

  constructor(workloadOpts, machineType) {
    this.workloadOpts = workloadOpts;
    this.machineType = machineType;

    let timeInterval;
    if (this.workloadOpts.endTime)
      timeInterval = this.workloadOpts.endTime - this.workloadOpts.startTime;
    else if (this.workloadOpts.duration)
      timeInterval = this.workloadOpts.duration;

    this.bulkWritesTimeStep = Number.MAX_SAFE_INTEGER;
    this.maxSampleStep = 1000;
    this.maxQueryStep = 1000;
    this.samples = {};
    for (let k in this.machineType.sampleIntervals) {
      this.bulkWritesTimeStep = Math.min(this.bulkWritesTimeStep, this.machineType.sampleIntervals[k]);
      this.maxSampleStep = Math.max(this.maxSampleStep, this.machineType.sampleIntervals[k]);
      this.samples[k] = Math.floor(timeInterval / this.machineType.sampleIntervals[k] * this.workloadOpts.machineUptime);
    }
    for (let k in this.machineType.queryIntervals) {
      this.maxQueryStep = Math.max(this.maxQueryStep, this.machineType.queryIntervals[k]);
    }

    this.totalSamples = 0;
    for (let k in this.samples) {
      this.totalSamples += this.samples[k];
    }
    this.totalSamples *= this.workloadOpts.machines.length;

    let activeMachines = Math.round(this.workloadOpts.machines.length * this.workloadOpts.machineUptime);
    let i = 0;
    this.machines = {};
    for (let machineId of this.workloadOpts.machines) {
      let writeDelay = timeInterval * (1 - this.workloadOpts.machineUptime) * Math.random();
      writeDelay = Math.round(writeDelay / this.bulkWritesTimeStep) * this.bulkWritesTimeStep;

      let writePhase = this.maxSampleStep * Math.random();
      writePhase = Math.round(writePhase / REAL_TIME_STEP) * REAL_TIME_STEP;

      let groups = {};
      for (let k in this.machineType.sampleIntervals)
        groups[k] = 0;

      this.machines[machineId] = {
        active: i < activeMachines,
        groups: groups,
        groupNames: Object.keys(groups),
        writeDelay: writeDelay,
        writePhase: writePhase
      };

      ++i;
    }

    this.readPhase = this.maxQueryStep * Math.random();
    this.readPhase = Math.round(this.readPhase / REAL_TIME_STEP) * REAL_TIME_STEP;

    this.machineIds = Object.keys(this.machines);

    this.absTime = this.workloadOpts.startTime;
    this.absDate = new Date(this.absTime);
  }

  bulkWrites() {
    let done = false;
    let relTime = 0;
    let i = 0;
    let j = 0;

    let rs = Readable({
      objectMode: true,
      highWaterMark: HIGH_WATERMARK
    });
    rs._read = (size) => {
      let readSamples = 0;
      while (!done) {
        done = true;
        for (; i < this.machineIds.length; ++i) {
          let id = this.machineIds[i];
          let machine = this.machines[id];

          for (; j < machine.groupNames.length; ++j) {
            let groupName = machine.groupNames[j];

            done = false;
            if ((relTime + machine.writeDelay) % this.machineType.sampleIntervals[groupName] === 0) {
              rs.push({
                id: id,
                groupName: groupName,
                sample: this.machineType.sample(id, groupName, this.absDate)
              });

              ++machine.groups[groupName];

              if (machine.groups[groupName] >= this.samples[groupName]) {
                delete machine.groups[groupName];
                machine.groupNames = Object.keys(machine.groups);
              }

              if (++readSamples >= size)
                return;
            }
          }

          j = 0;
        }

        i = 0;
        this.absTime += this.bulkWritesTimeStep;
        this.absDate = new Date(this.absTime);
        relTime += this.bulkWritesTimeStep;
      }

      rs.push(null);
    };

    return rs;
  }

  realTimeReads() {
    let rs = Readable({
      objectMode: true,
      highWaterMark: HIGH_WATERMARK
    });

    let pushed = false;
    let lastSize = 0;
    let queue = [];

    let realTimeInterval = setInterval(() => {
      for (let query in this.machineType.queryIntervals) {
        if ((this.absTime + this.readPhase) % this.machineType.queryIntervals[query] === 0)
          queue.push(this.machineType.query(query, this.absDate));
      }

      if (!pushed && queue.length > 0)
        rs._read(lastSize);

      this.absTime += REAL_TIME_STEP;
      this.absDate = new Date(this.absTime);
    }, REAL_TIME_STEP);

    rs._read = (size) => {
      lastSize = size;
      pushed = false;
      for (let i = 0; i < size && i < queue.length; ++i) {
        pushed = true;
        rs.push(queue.shift());
      }
    };

    setTimeout(() => {
      clearInterval(realTimeInterval);
      rs.push(null);
    }, this.workloadOpts.duration);

    return rs;
  }

  realTimeWrites() {
    let rs = Readable({
      objectMode: true,
      highWaterMark: HIGH_WATERMARK
    });

    let pushed = false;
    let lastSize = 0;
    let queue = [];

    let realTimeInterval = setInterval(() => {
      for (let machineId in this.machines) {
        let writePhase = this.machines[machineId].writePhase;
        for (let groupName in this.machineType.sampleIntervals) {
          if ((this.absTime + writePhase) % this.machineType.sampleIntervals[groupName] === 0) {
            queue.push({
              id: machineId,
              groupName: groupName,
              sample: this.machineType.sample(machineId, groupName, this.absDate)
            });
          }
        }
      }

      if (!pushed && queue.length > 0)
        rs._read(lastSize);

      this.absTime += REAL_TIME_STEP;
      this.absDate = new Date(this.absTime);
    }, REAL_TIME_STEP);

    rs._read = (size) => {
      lastSize = size;
      pushed = false;
      for (let i = 0; i < size && i < queue.length; ++i) {
        pushed = true;
        rs.push(queue.shift());
      }
    };

    setTimeout(() => {
      clearInterval(realTimeInterval);
      rs.push(null);
    }, this.workloadOpts.duration);

    return rs;
  }

};