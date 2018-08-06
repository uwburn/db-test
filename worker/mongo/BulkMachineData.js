"use strict";

const uuidv4 = require('uuid/v4');
const MongoClient = require('mongodb').MongoClient;
const intervalPromise = require('interval-promise')

const MAX_WORKER_DELAY = 10000;

const INTERVALS = {
    status: 30000,
    counters: 120000,
    setup: 300000,
    temperature: 1000,
    pressure: 1000,
    alarm: 1800000
}

const TIME_STEP = Math.min(
    INTERVALS.status,
    INTERVALS.counters,
    INTERVALS.setup,
    INTERVALS.temperature,
    INTERVALS.pressure,
    INTERVALS.alarm
);

const INSERTION_INTERVAL = 1000;
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
        }

        this.totalSamples = 0;
        for (let k in this.samples) {
            this.totalSamples += this.samples[k];
        }
        this.totalSamples *= this.workloadOpts.machines;
        this.maxSamplesPerCycle = this.workloadOpts.ratePerSecond / INSERTION_INTERVAL * 1000;

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

        this.cycles = 0;
        this.samplesProduced = 0;
        this.errors = 0;
    }

    log() {
        console.log(`Cycles: ${this.cycles}, samples: ${this.samplesProduced}/${this.totalSamples}, errors: ${this.errors}, ${Math.round(this.samplesProduced/this.totalSamples*100)}%`);

        this.mqttClient.publish(`worker/${this.workerId}/work/${this.id}/log`, JSON.stringify({
            time: new Date().getTime(),
            totalSamples: this.totalSamples,
            samples: this.samplesProduced,
            cycles: this.cycles,
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

    async _run() {
        await new Promise((resolve) => {
            let done = false;
            let absTime = this.workloadOpts.startTime;
            let relTime = 0;
            let i = 0;
            let j = 0;

            let logInterval;

            console.log(`Waiting worker delay (${Math.round(this.workerDelay / 1000)} s) to pass... `);

            setTimeout(() => {
                this.startTime = new Date().getTime();

                logInterval = setInterval(this.log.bind(this), LOG_INTERVAL);

                intervalPromise(async (iterationNumber, stop) => {
                    ++this.cycles;
                    let cycleSamples = 0;
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
                                    this.sample(id, groupName, absTime);
                                    ++machine.groups[groupName];
                                    ++cycleSamples;
                                    ++this.samplesProduced;
        
                                    if (machine.groups[groupName] >= this.samples[groupName])
                                        delete machine.groups[groupName];
        
                                    if (cycleSamples >= this.maxSamplesPerCycle)
                                        return;
                                }
                            }

                            j = 0;
                        }
        
                        i = 0;
                        absTime += TIME_STEP;
                        relTime += TIME_STEP;
                    }

                    this.endTime = new Date().getTime();
                    stop();
                    clearInterval(logInterval);
                    
                    this.log();
                    console.log(`Workload completed`);
                    resolve();
                }, INSERTION_INTERVAL);
            }, this.workerDelay);
        });
    }

    async sample(id, groupName, absTime) {
        let sample = this.sampleMethods[groupName](id, groupName, absTime);
        this.recordMethods[groupName](id, groupName, sample);
    }

    statusSample(id, groupName, absTime) {
        let sample = {
            deviceType: this.workloadOpts.deviceType,
            device: id,
            originalTime: absTime,
            time: absTime
        };

        sample[groupName] = {
            group: {
                time: absTime,
                value: groupName
            }
        }

        return sample;
    }

    countersSample(id, groupName, absTime) {
        let sample = {
            deviceType: this.workloadOpts.deviceType,
            device: id,
            time: absTime
        };

        sample[groupName] = {
            group: {
                time: absTime,
                value: groupName
            }
        }

        return sample;
    }

    setupSample(id, groupName, absTime) {
        let sample = {
            deviceType: this.workloadOpts.deviceType,
            device: id,
            time: absTime
        };

        sample[groupName] = {
            group: {
                time: absTime,
                value: groupName
            }
        }

        return sample;
    }

    temperatureSample(id, groupName, absTime) {
        let sample = {
            deviceType: this.workloadOpts.deviceType,
            device: id,
            time: absTime,
            temperature: {
                time: absTime,
                value: Math.random() * 100 + 50
            }
        };

        return sample;
    }

    pressureSample(id, groupName, absTime) {
        let sample = {
            deviceType: this.workloadOpts.deviceType,
            device: id,
            time: absTime,
            pressure: {
                time: absTime,
                vale: Math.random() * 4 +1
            }
        };

        return sample;
    }

    alarmSample(id, groupName, absTime) {
        let interval = {
            _id: uuidv4(),
            deviceType: this.workloadOpts.deviceType,
            device: id,
            startTime: absTime,
            endTime: absTime + Math.round(Math.number * 300 + 60) * 1000,
            value: Math.ceil(Math.random() * 100).toString()
        }

        return interval;
    }

    async recordTimeComplex(id, groupName, sample) {
        let criteria = {
            deviceType: sample.deviceType,
            device: sample.id,
            time: sample.time
        };

        let options = {
            upsert: true
        }

        await this.timeComplexColl.update(criteria, sample, options);
    }

    async recordInterval(id, groupName, sample) {

    }

}
