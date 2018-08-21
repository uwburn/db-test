"use strict";

const { Readable } = require('stream');
const _ = require("lodash");

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
    let machines = _.cloneDeep(this.machines);
    let samples = _.cloneDeep(this.samples);

    let samplesCount = 0;

    let percent = () => {
      let percent = Math.round(samplesCount / this.totalSamples * 100);
      if (isNaN(percent))
        percent = 100;

      return percent;
    };

    let result = {
      stream: Readable({
        objectMode: true,
        highWaterMark: HIGH_WATERMARK
      }),
      percent: percent
    };

    let done = false;
    let absTime = this.workloadOpts.startTime;
    let absDate = new Date(this.workloadOpts.startTime);
    let relTime = 0;
    let i = 0;
    let j = 0;

    result.stream._read = (size) => {
      let readSamples = 0;
      while (!done) {
        done = true;
        for (; i < this.machineIds.length; ++i) {
          let id = this.machineIds[i];
          let machine = machines[id];

          for (; j < machine.groupNames.length; ++j) {
            let groupName = machine.groupNames[j];

            done = false;
            if ((relTime + machine.writeDelay) % this.machineType.sampleIntervals[groupName] === 0) {
              result.stream.push({
                id: id,
                groupName: groupName,
                sample: this.machineType.sample(id, groupName, absDate)
              });

              ++machine.groups[groupName];

              if (machine.groups[groupName] >= samples[groupName]) {
                delete machine.groups[groupName];
                machine.groupNames = Object.keys(machine.groups);
              }

              ++samplesCount;

              if (++readSamples >= size)
                return;
            }
          }

          j = 0;
        }

        i = 0;
        absTime += this.bulkWritesTimeStep;
        absDate = new Date(absTime);
        relTime += this.bulkWritesTimeStep;
      }

      result.stream.push(null);
    };

    return result;
  }

  realTimeReads() {
    let absTime = this.workloadOpts.startTime;
    let absDate = new Date(this.workloadOpts.startTime);

    let percent = () => {
      let percent = Math.round((absTime - this.workloadOpts.startTime) / this.workloadOpts.duration * 100);
      if (isNaN(percent))
        percent = 100;

      return percent;
    };

    let result = {
      stream: Readable({
        objectMode: true,
        highWaterMark: HIGH_WATERMARK
      }),
      percent: percent
    };

    let pushed = false;
    let lastSize = 0;
    let queue = [];

    let realTimeInterval = setInterval(() => {
      for (let query in this.machineType.queryIntervals) {
        if ((absTime + this.readPhase) % this.machineType.queryIntervals[query] === 0)
          queue.push(this.machineType.query(query, absDate));
      }

      if (!pushed && queue.length > 0)
        result.stream._read(lastSize);

      absTime += REAL_TIME_STEP;
      absDate = new Date(absTime);
    }, REAL_TIME_STEP);

    result.stream._read = (size) => {
      lastSize = size;
      pushed = false;
      for (let i = 0; i < size && i < queue.length; ++i) {
        pushed = true;
        result.stream.push(queue.shift());
      }
    };

    setTimeout(() => {
      clearInterval(realTimeInterval);
      result.stream.push(null);
    }, this.workloadOpts.duration);

    return result;
  }

  realTimeWrites() {
    let absTime = this.workloadOpts.startTime;
    let absDate = new Date(this.workloadOpts.startTime);

    let percent = () => {
      let percent = Math.round((absTime - this.workloadOpts.startTime) / this.workloadOpts.duration * 100);
      if (isNaN(percent))
        percent = 100;

      return percent;
    };

    let result = {
      stream: Readable({
        objectMode: true,
        highWaterMark: HIGH_WATERMARK
      }),
      percent: percent
    };

    let pushed = false;
    let lastSize = 0;
    let queue = [];

    let realTimeInterval = setInterval(() => {
      for (let machineId in this.machines) {
        let writePhase = this.machines[machineId].writePhase;
        for (let groupName in this.machineType.sampleIntervals) {
          if ((absTime + writePhase) % this.machineType.sampleIntervals[groupName] === 0) {
            queue.push({
              id: machineId,
              groupName: groupName,
              sample: this.machineType.sample(machineId, groupName, absDate)
            });
          }
        }
      }

      if (!pushed && queue.length > 0)
        result.stream._read(lastSize);

      absTime += REAL_TIME_STEP;
      absDate = new Date(absTime);
    }, REAL_TIME_STEP);

    result.stream._read = (size) => {
      lastSize = size;
      pushed = false;
      for (let i = 0; i < size && i < queue.length; ++i) {
        pushed = true;
        result.stream.push(queue.shift());
      }
    };

    setTimeout(() => {
      clearInterval(realTimeInterval);
      result.stream.push(null);
    }, this.workloadOpts.duration);

    return result;
  }

};