"use strict";

const { Readable, Writable } = require('stream');
const uuidv4 = require('uuid/v4');
const MongoClient = require('mongodb').MongoClient;

const STREAM_HIGH_WATERMARK = 256;
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
        this.totalSamples *= this.workloadOpts.machines;
        this.samplesRecorded = 0;

        this.machines = {};
        for (let i = 0; i < this.workloadOpts.machines; ++i) {
            let machineDelay = timeInterval * (1 - this.workloadOpts.machineUptime) * Math.random();
            machineDelay = Math.round(machineDelay / TIME_STEP) * TIME_STEP;

            this.machines[uuidv4()] = {
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

        this.recordMethods = {
            status: this.recordTimeComplex.bind(this),
            counters: this.recordTimeComplex.bind(this),
            setup: this.recordTimeComplex.bind(this),
            temperature: this.recordTimeComplex.bind(this),
            pressure: this.recordTimeComplex.bind(this),
            alarm: this.recordInterval.bind(this),
        };

        this.samplesRecorded = 0;
        this.errors = 0;
    }

    log() {
        console.log(`Records: ${this.samplesRecorded}/${this.totalSamples}, errors: ${this.errors}, ${Math.round(this.samplesRecorded/this.totalSamples*100)}%`);

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
        this.mongoClient = await MongoClient.connect(this.databaseOpts.url);
        this.db = this.mongoClient.db(`db-test`);
        this.timeComplexColl = this.db.collection(`timeComplex`);
        this.intervalColl = this.db.collection(`interval`);

        await this.timeComplexColl.ensureIndex(`deviceType`);
        await this.timeComplexColl.ensureIndex(`device`);
        await this.timeComplexColl.ensureIndex(`time`);

        await this.intervalColl.ensureIndex(`deviceType`);
        await this.intervalColl.ensureIndex(`device`);
        await this.intervalColl.ensureIndex(`startTime`);
        await this.intervalColl.ensureIndex(`endTime`);
    }

    async cleanup() {
        this.mongoClient.close();
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
        highWaterMark: STREAM_HIGH_WATERMARK
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

    mongoRecordStream() {
      let ws = Writable({
        objectMode: true,
        highWaterMark: STREAM_HIGH_WATERMARK
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

                let mongoRecordStream = this.mongoRecordStream();
                this.machineDataStream().pipe(mongoRecordStream);

                mongoRecordStream.once("finish", () => {
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
            endTime: new Date(absDate.getTime() + Math.round(Math.number * 300 + 60) * 1000),
            value: Math.ceil(Math.random() * 100).toString()
        };
    }

  async record(id, groupName, sample) {
    return this.recordMethods[groupName](id, groupName, sample);
  }

    async recordTimeComplex(id, groupName, sample) {
        let criteria = {
            deviceType: sample.deviceType,
            device: sample.device,
            time: sample.time
        };

        let update = {
            $setOnInsert: { 
                deviceType: sample.deviceType,
                device: sample.device,
                time: sample.time
            },
            $set: { }
        };

        update.$set[groupName] = sample[groupName];

        let options = {
            upsert: true
        };

        await this.timeComplexColl.update(criteria, update, options);
    }

    async recordInterval(id, groupName, sample) {
      let criteria = {
        _id: sample.id
      };

      let update = {
        $setOnInsert: {
          deviceType: sample.deviceType,
          device: sample.device,
          time: sample.time
        },
        $set: {
          value: sample.value
        }
      };

      let options = {
        upsert: true
      };

      await this.intervalColl.update(criteria, update, options);
    }

};
