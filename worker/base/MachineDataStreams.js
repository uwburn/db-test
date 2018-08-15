"use strict";

const { Readable } = require('stream');

const HIGH_WATERMARK = 256;
const REAL_TIME_STEP = 1000;

module.exports = class MachineDataStreams {

  constructor(workloadOpts, sampler) {
    this.workloadOpts = workloadOpts;
    this.eventIntervals = sampler.nominalIntervals;
    this.sampler = sampler;

    let timeInterval;
    if (this.workloadOpts.endTime)
      timeInterval = this.workloadOpts.endTime - this.workloadOpts.startTime;
    else if (this.workloadOpts.duration)
      timeInterval = this.workloadOpts.duration;

    this.timeStep = Number.MAX_SAFE_INTEGER;
    this.maxStep = 1000;
    this.samples = {};
    for (let k in this.eventIntervals) {
      this.timeStep = Math.min(this.timeStep, this.eventIntervals[k]);
      this.maxStep = Math.max(this.maxStep, this.eventIntervals[k]);
      this.samples[k] = Math.floor(timeInterval / this.eventIntervals[k] * this.workloadOpts.machineUptime);
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
      let machineDelay = timeInterval * (1 - this.workloadOpts.machineUptime) * Math.random();
      machineDelay = Math.round(machineDelay / this.timeStep) * this.timeStep;

      let machinePhase = this.maxStep * Math.random();
      machinePhase = Math.round(machinePhase / this.timeStep) * this.timeStep;

      let groups = {};
      for (let k in this.eventIntervals)
        groups[k] = 0;

      this.machines[machineId] = {
        active: i < activeMachines,
        groups: groups,
        groupNames: Object.keys(groups),
        machineDelay: machineDelay,
        machinePhase: machinePhase
      }

      ++i;
    }

    this.machineIds = Object.keys(this.machines);

    this.absTime = this.workloadOpts.startTime;
    this.absDate = new Date(this.absTime);
  }

  bulkStream() {
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
            if ((relTime + machine.machineDelay) % this.eventIntervals[groupName] === 0) {
              rs.push({
                id: id,
                groupName: groupName,
                sample: this.sampler.sample(id, groupName, this.absDate)
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
        this.absTime += this.timeStep;
        this.absDate = new Date(this.absTime);
        relTime += this.timeStep;
      }

      rs.push(null);
    };

    return rs;
  }

  realTimeStream() {
    let rs = Readable({
      objectMode: true,
      highWaterMark: HIGH_WATERMARK
    });

    let pushed = false;
    let lastSize = 0;
    let queue = [];

    let realTimeInterval = setInterval(() => {
      for (let machineId in this.machines) {
        if (!this.machines[machineId].active)
          break;

        let machinePhase = this.machines[machineId].machinePhase;
        for (let groupName in this.eventIntervals) {
          if ((this.absTime + machinePhase) % this.eventIntervals[groupName] === 0) {
            queue.push({
              id: machineId,
              groupName: groupName,
              sample: this.sampler.sample(machineId, groupName, this.absDate)
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
      finished = true;
      clearInterval(realTimeInterval);
      rs.push(null);
    }, this.workloadOpts.duration);

    return rs;
  }

}