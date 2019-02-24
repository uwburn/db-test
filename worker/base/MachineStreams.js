"use strict";

const { Readable } = require('stream');
const _ = require("lodash");

const HIGH_WATERMARK = 256;
const REAL_TIME_STEP = 1000;

function gcd(a, b) {
  if (!b) {
    return a;
  }

  return gcd(b, a % b);
};

module.exports = class MachineStreams {

  constructor(workloadOpts, source) {
    this.workloadOpts = workloadOpts;
    this.source = source;

    let timeInterval;
    if (this.workloadOpts.endTime)
      timeInterval = this.workloadOpts.endTime - this.workloadOpts.startTime;
    else if (this.workloadOpts.duration)
      timeInterval = this.workloadOpts.duration;

    this.bulkWritesTimeStep = this.source.sampleIntervals[0];
    this.bulkReadsTimeStep = this.source.queryIntervals[0];
    this.maxSampleStep = 1000;
    this.maxQueryStep = 1000;
    this.samples = {};
    this.queries = {};
    for (let k in this.source.sampleIntervals) {
      this.bulkWritesTimeStep = gcd(this.bulkWritesTimeStep, this.source.sampleIntervals[k]);
      this.maxSampleStep = Math.max(this.maxSampleStep, this.source.sampleIntervals[k]);
      this.samples[k] = Math.floor(timeInterval / this.source.sampleIntervals[k] * this.workloadOpts.machineUptime);
    }
    for (let k in this.source.queryIntervals) {
      this.bulkReadsTimeStep = gcd(this.bulkReadsTimeStep, this.source.queryIntervals[k]);
      this.maxQueryStep = Math.max(this.maxQueryStep, this.source.queryIntervals[k]);
      this.queries[k] = Math.floor(timeInterval / this.source.queryIntervals[k]);
    }

    this.totalSamples = 0;
    for (let k in this.samples)
      this.totalSamples += this.samples[k];
    this.totalSamples *= this.workloadOpts.machines.length;

    this.totalQueries = 0;
    for (let k in this.queries)
      this.totalQueries += this.queries[k];

    if (this.totalQueries > this.workloadOpts.bulkReadsLimit) {
      for (let k in this.queries)
        this.queries[k] = Math.round(this.queries[k] / this.totalQueries * this.workloadOpts.bulkReadsLimit);

      this.totalQueries = 0;
      for (let j in this.queries)
        this.totalQueries += this.queries[j];

      let diff = this.workloadOpts.bulkReadsLimit - this.totalQueries;
      let queryKeys = Object.keys(this.queries);
      for (let i = 0; i < diff; ++i) {
        let h = queryKeys[i % queryKeys.length];
        this.queries[h]++;
      }

      this.totalQueries = this.workloadOpts.bulkReadsLimit;
    }

    let activeMachines = Math.round(this.workloadOpts.machines.length * this.workloadOpts.machineUptime);
    let i = 0;
    this.machines = {};
    for (let machineId of this.workloadOpts.machines) {
      let writeDelay = timeInterval * (1 - this.workloadOpts.machineUptime) * Math.random();
      writeDelay = Math.round(writeDelay / this.bulkWritesTimeStep) * this.bulkWritesTimeStep;

      let writePhase = this.maxSampleStep * Math.random();
      writePhase = Math.round(writePhase / REAL_TIME_STEP) * REAL_TIME_STEP;

      let groups = {};
      for (let k in this.source.sampleIntervals)
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

  bulkReads() {
    let queries = _.cloneDeep(this.queries);

    let queriesCount = 0;

    let percent = () => {
      let percent = Math.round(queriesCount / this.totalQueries * 100);
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

    const advanceTime = () => {
      absTime += this.bulkReadsTimeStep;
      absDate = new Date(absTime);
      relTime += this.bulkReadsTimeStep;
    };

    let queryNames = Object.keys(queries);

    result.stream._read = (size) => {
      let readQueries = 0;
      while (!done) {
        done = i == 0;
        for (; i < queryNames.length; ++i) {
          let queryName = queryNames[i];
          if (queries[queryName] <= 0)
            continue;

          done = false;

          if (relTime % this.source.queryIntervals[queryName] === 0) {
            let pushRes = result.stream.push(this.source.query(queryName, absDate));

            --queries[queryName]
            ++queriesCount;

            if (++readQueries >= size || !pushRes) {
              i = ++i % queryNames.length || 0;

              if (i === 0)
                advanceTime();

              return;
            }
          }
        }

        i = 0;
        advanceTime();
      }

      result.stream.push(null);
    };

    return result;
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

    let initialDelay = Number.MAX_SAFE_INTEGER;
    for (let k in machines)
      initialDelay = Math.min(machines[k].writeDelay, initialDelay);

    let done = false;
    let absTime = this.workloadOpts.startTime + initialDelay;
    let absDate = new Date(absTime);
    let relTime = initialDelay;
    let i = 0;
    let j = 0;

    const advanceTime = () => {
      absTime += this.bulkWritesTimeStep;
      absDate = new Date(absTime);
      relTime += this.bulkWritesTimeStep;
    };

    result.stream._read = (size) => {
      let readSamples = 0;
      while (!done) {
        done = (i == 0 && j == 0);
        for (; i < this.machineIds.length; ++i) {
          let id = this.machineIds[i];
          let machine = machines[id];

          if (machine.writeDelay > relTime) {
            done = false;
            continue;
          }

          for (; j < machine.groupNames.length; ++j) {
            let groupName = machine.groupNames[j];
            if (machine.groups[groupName] >= samples[groupName])
              continue;

            done = false;
            let interval = this.source.sampleIntervals[groupName];
            if ((relTime + machine.writeDelay) % interval === 0) {
              let pushRes = result.stream.push({
                id: id,
                groupName: groupName,
                sample: this.source.sample(id, groupName, absDate),
                interval: interval
              });

              ++machine.groups[groupName];

              ++samplesCount;

              if (++readSamples >= size || !pushRes) {
                j = ++j % machine.groupNames.length || 0;

                if (j === 0)
                  i = ++i % this.machineIds.length || 0;

                if (i === 0 && j === 0)
                  advanceTime();

                return;
              }
            }
          }

          j = 0;
        }

        i = 0;
        advanceTime();
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
      for (let query in this.source.queryIntervals) {
        if ((absTime + this.readPhase) % this.source.queryIntervals[query] === 0)
          queue.push(this.source.query(query, absDate));
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
        if (!result.stream.push(queue.shift()))
          return;
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
        for (let groupName in this.source.sampleIntervals) {
          let interval = this.source.sampleIntervals[groupName];
          if ((absTime + writePhase) % interval === 0) {
            queue.push({
              id: machineId,
              groupName: groupName,
              sample: this.source.sample(machineId, groupName, absDate),
              interval: interval
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
        if (!result.stream.push(queue.shift()))
          return;
      }
    };

    setTimeout(() => {
      clearInterval(realTimeInterval);
      result.stream.push(null);
    }, this.workloadOpts.duration);

    return result;
  }

};