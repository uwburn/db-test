"use strict";

const { Readable, Writable } = require('stream');
const uuidv4 = require('uuid/v4');

const MAX_WORKER_DELAY = 10000;

const INTERVALS = {
    status: 30000,
    counters: 120000,
    setup: 300000,
    temperature: 1000,
    pressure: 1000,
    alarm: 1800000
};

const TIME_STEP = Math.min(
    INTERVALS.status,
    INTERVALS.counters,
    INTERVALS.setup,
    INTERVALS.temperature,
    INTERVALS.pressure,
    INTERVALS.alarm
);

const LOG_INTERVAL = 10000;

module.exports = class BulkMachineData {
    
    constructor(id, workerId, workloadOpts, databaseOpts, mqttClient) {
        this.id = id;
        this.workerId = workerId;
        this.workloadOpts = workloadOpts;
        this.databaseOpts = databaseOpts;
        this.mqttClient = mqttClient;

        this.workerDelay = Math.round(Math.random() * MAX_WORKER_DELAY);

        let timeInterval = this.workloadOpts.endTime - this.workloadOpts.startTime;

        this.samples = {
            status: Math.floor(timeInterval / INTERVALS.status * this.workloadOpts.machineUptime),
            counters: Math.floor(timeInterval / INTERVALS.counters * this.workloadOpts.machineUptime),
            setup: Math.floor(timeInterval / INTERVALS.setup * this.workloadOpts.machineUptime),
            temperature: Math.floor(timeInterval / INTERVALS.temperature * this.workloadOpts.machineUptime),
            pressure: Math.floor(timeInterval / INTERVALS.pressure * this.workloadOpts.machineUptime),
            alarm: Math.floor(timeInterval / INTERVALS.alarm * this.workloadOpts.machineUptime)
        };

        this.totalSamples = 0;
        for (let k in this.samples) {
            this.totalSamples += this.samples[k];
        }
        this.totalSamples *= this.workloadOpts.machines.length;
        this.samplesRecorded = 0;

        this.machines = {};
        for (let machineId of this.workloadOpts.machines) {
            let machineDelay = timeInterval * (1 - this.workloadOpts.machineUptime) * Math.random();
            machineDelay = Math.round(machineDelay / TIME_STEP) * TIME_STEP;

            this.machines[machineId] = {
                groups: {
                    status: 0,
                    counters: 0,
                    setup: 0,
                    temperature: 0,
                    pressure: 0,
                    alarm: 0
                },
                machineDelay: machineDelay
            }
        }

        this.sampleMethods = {
            status: this.statusSample.bind(this),
            counters: this.countersSample.bind(this),
            setup: this.setupSample.bind(this),
            temperature: this.temperatureSample.bind(this),
            pressure: this.pressureSample.bind(this),
            alarm: this.alarmSample.bind(this)
        };

        this.samplesRecorded = 0;
        this.errors = 0;
    }

    log() {
      let percent = Math.round(this.samplesRecorded/this.totalSamples*100);
      if (isNaN(percent))
        percent = 100;

        console.log(`Records: ${this.samplesRecorded}/${this.totalSamples}, errors: ${this.errors}, ${percent}%`);

        this.mqttClient.publish(`worker/${this.workerId}/work/${this.id}/log`, JSON.stringify({
            time: new Date().getTime(),
            totalSamples: this.totalSamples,
            samplesRecorded: this.samplesRecorded,
            errors: this.errors,
            startTime: this.startTime,
            endTime: this.endTime
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
          ++this.samplesRecorded;
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

    sample(id, groupName, absDate) {
        return this.sampleMethods[groupName](id, groupName, absDate);
    }

    statusSample(id, groupName, absDate) {
        let sample = {
            deviceType: this.workloadOpts.machineTypeId,
            device: id,
            time: absDate
        };

        sample[groupName] = {
            group: {
                time: absDate,
                value: groupName
            }
        };

        return sample;
    }

    countersSample(id, groupName, absDate) {
        let sample = {
            deviceType: this.workloadOpts.machineTypeId,
            device: id,
            time: absDate
        };

        sample[groupName] = {
            group: {
                time: absDate,
                value: groupName
            }
        };

        return sample;
    }

    setupSample(id, groupName, absDate) {
        let sample = {
            deviceType: this.workloadOpts.machineTypeId,
            device: id,
            time: absDate
        };

        sample[groupName] = {
            group: {
                time: absDate,
                value: groupName
            }
        };

        return sample;
    }

    temperatureSample(id, groupName, absDate) {
        return {
            deviceType: this.workloadOpts.machineTypeId,
            device: id,
            time: absDate,
            temperature: {
                time: absDate,
                value: Math.random() * 100 + 50
            }
        };
    }

    pressureSample(id, groupName, absDate) {
        return {
            deviceType: this.workloadOpts.machineTypeId,
            device: id,
            time: absDate,
            pressure: {
                time: absDate,
                value: Math.random() * 4 +1
            }
        };
    }

    alarmSample(id, groupName, absDate) {
        return {
            _id: uuidv4(),
            deviceType: this.workloadOpts.machineTypeId,
            device: id,
            startTime: absDate,
            endTime: new Date(absDate.getTime() + Math.round(Math.random() * 300 + 60) * 1000),
            value: Math.ceil(Math.random() * 100).toString()
        };
    }

  async record(id, groupName, sample) {
    throw new Error("Base class doesn't implement record method");
  }

};
