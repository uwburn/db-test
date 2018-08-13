"use strict";

const { Readable, Writable } = require('stream');
const uuidv4 = require('uuid/v4');

const MAX_WORKER_DELAY = 10000;

const INTERVALS = {
  status: 3600000,
  counters: 120000,
  setup: 300000,
  temperatureProbe1: 1000,
  temperatureProbe2: 1000,
  alarm: 1800000
};

const TIME_STEP = Math.min(
  INTERVALS.status,
  INTERVALS.counters,
  INTERVALS.setup,
  INTERVALS.temperatureProbe1,
  INTERVALS.temperatureProbe2,
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
      temperatureProbe1: Math.floor(timeInterval / INTERVALS.temperatureProbe1 * this.workloadOpts.machineUptime),
      temperatureProbe2: Math.floor(timeInterval / INTERVALS.temperatureProbe2 * this.workloadOpts.machineUptime),
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
          status: 0,
          counters: 0,
          setup: 0,
          temperatureProbe1: 0,
          temperatureProbe2: 0,
          alarm: 0
        },
        machineDelay: machineDelay
      }
    }

    this.sampleMethods = {
      status: this.statusSample.bind(this),
      counters: this.countersSample.bind(this),
      setup: this.setupSample.bind(this),
      temperatureProbe1: this.temperatureProbe1Sample.bind(this),
      temperatureProbe2: this.temperatureProbe2Sample.bind(this),
      alarm: this.alarmSample.bind(this)
    };

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

  sample(id, groupName, absDate) {
    return this.sampleMethods[groupName](id, groupName, absDate);
  }

  statusSample(id, groupName, absDate) {
    return {
      id: uuidv4(),
      deviceType: this.workloadOpts.machineTypeId,
      device: id,
      startTime: absDate,
      endTime: new Date(absDate.getTime() + Math.round(Math.random() * 300 + 60) * 1000),
      value: Math.random() > 0.5 ? "WORKING" : "IDLE"
    };
  }

  countersSample(id, groupName, absDate) {
    return {
      deviceType: this.workloadOpts.machineTypeId,
      device: id,
      time: absDate,
      counters: {
        time: absDate,
        value: {
          activeEnergyConsumed: Math.round(Math.random() * 1000000),
          reactiveEnergyProduced: Math.round(Math.random() * 1000000),
          totalLengthProcessed: Math.random() * 1000000,
          partialLengthProcessed: Math.random() * 1000000,
          totalWorkedTime: Math.round(Math.random() * 1000000),
          partialWorkedTime: Math.round(Math.random() * 1000000),
          totalPartsProcessed: Math.round(Math.random() * 1000000),
          partialPartProcessed: Math.round(Math.random() * 1000000),
          clutches: [
            {
              cycles: Math.round(Math.random() * 10000)
            },
            {
              cycles: Math.round(Math.random() * 10000)
            }
          ],
          oilPumps: [
            {
              cycles: Math.round(Math.random() * 10000),
              workingTime: Math.round(Math.random() * 1000000),
            },
            {
              cycles: Math.round(Math.random() * 10000),
              workingTime: Math.round(Math.random() * 1000000),
            },
            {
              cycles: Math.round(Math.random() * 10000),
              workingTime: Math.round(Math.random() * 1000000),
            },
            {
              cycles: Math.round(Math.random() * 10000),
              workingTime: Math.round(Math.random() * 1000000),
            }
          ],
          belts: [
            {
              failures: Math.round(Math.random() * 100),
              workingTime: Math.round(Math.random() * 1000000),
            },
            {
              failures: Math.round(Math.random() * 100),
              workingTime: Math.round(Math.random() * 1000000),
            },
            {
              failures: Math.round(Math.random() * 100),
              workingTime: Math.round(Math.random() * 1000000),
            },
            {
              failures: Math.round(Math.random() * 100),
              workingTime: Math.round(Math.random() * 1000000),
            },
            {
              failures: Math.round(Math.random() * 100),
              workingTime: Math.round(Math.random() * 1000000),
            },
            {
              failures: Math.round(Math.random() * 100),
              workingTime: Math.round(Math.random() * 1000000),
            }
          ],
          lamps: [
            {
              cycles: Math.round(Math.random() * 10000),
              failures: Math.round(Math.random() * 100),
              workingTime: Math.round(Math.random() * 1000000),
            },
            {
              cycles: Math.round(Math.random() * 10000),
              failures: Math.round(Math.random() * 100),
              workingTime: Math.round(Math.random() * 1000000),
            }
          ],
          drives: [
            {
              cycles: Math.round(Math.random() * 10000),
            },
            {
              cycles: Math.round(Math.random() * 10000),
            },
            {
              cycles: Math.round(Math.random() * 10000),
            },
            {
              cycles: Math.round(Math.random() * 10000),
            },
            {
              cycles: Math.round(Math.random() * 10000),
            },
            {
              cycles: Math.round(Math.random() * 10000),
            },
            {
              cycles: Math.round(Math.random() * 10000),
            },
            {
              cycles: Math.round(Math.random() * 10000),
            }
          ]
        }
      }
    };
  }

  setupSample(id, groupName, absDate) {
    return {
      deviceType: this.workloadOpts.machineTypeId,
      device: id,
      time: absDate,
      setup: {
        time: absDate,
        value: {
          partDiameter: Math.round(Math.random() * 500),
          partThickness: Math.round(Math.random() * 10),
          partLength: Math.round(Math.random() * 5000),
          partType: Math.round(Math.random() * 5),
          partCode: Math.round(Math.random() * 10000).toString(),
          batchCode: Math.round(Math.random() * 10000).toString(),
          heaters: [
            {
              preheatPosition: Math.round(Math.random() * 1000),
              heatPosition: Math.round(Math.random() * 1000),
              temperature: Math.round(Math.random() * 200),
              preheatTime: Math.round(Math.random() * 10),
              heatTime: Math.round(Math.random() * 20)
            },
            {
              preheatPosition: Math.round(Math.random() * 1000),
              heatPosition: Math.round(Math.random() * 1000),
              temperature: Math.round(Math.random() * 200),
              preheatTime: Math.round(Math.random() * 10),
              heatTime: Math.round(Math.random() * 20)
            }
          ],
          drives: [
            {
              time: Math.round(Math.random() * 10),
              delay: Math.round(Math.random() * 5)
            },
            {
              time: Math.round(Math.random() * 10),
              delay: Math.round(Math.random() * 5)
            },
            {
              time: Math.round(Math.random() * 10),
              delay: Math.round(Math.random() * 5)
            },
            {
              time: Math.round(Math.random() * 10),
              delay: Math.round(Math.random() * 5)
            },
            {
              time: Math.round(Math.random() * 10),
              delay: Math.round(Math.random() * 5)
            },
            {
              time: Math.round(Math.random() * 10),
              delay: Math.round(Math.random() * 5)
            },
            {
              time: Math.round(Math.random() * 10),
              delay: Math.round(Math.random() * 5)
            },
            {
              time: Math.round(Math.random() * 10),
              delay: Math.round(Math.random() * 5)
            }
          ]
        }
      }
    };
  }

  temperatureProbe1Sample(id, groupName, absDate) {
    return {
      deviceType: this.workloadOpts.machineTypeId,
      device: id,
      time: absDate,
      temperatureProbe1: {
        time: absDate,
        value: Math.random() * 100 + 50
      }
    };
  }

  temperatureProbe2Sample(id, groupName, absDate) {
    return {
      deviceType: this.workloadOpts.machineTypeId,
      device: id,
      time: absDate,
      temperatureProbe2: {
        time: absDate,
        value: Math.random() * 100 + 50
      }
    };
  }

  alarmSample(id, groupName, absDate) {
    return {
      id: uuidv4(),
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
