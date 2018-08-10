"use strict";

const MongoClient = require('mongodb').MongoClient;

const BaseBulkMachineData = require("../base/BulkMachineData");

module.exports = class BulkMachineData extends BaseBulkMachineData {
    
    constructor(id, workerId, workloadOpts, databaseOpts, mqttClient) {
      super(id, workerId, workloadOpts, databaseOpts, mqttClient);

        this.recordMethods = {
            status: this.recordInterval.bind(this),
            counters: this.recordTimeComplex.bind(this),
            setup: this.recordTimeComplex.bind(this),
            temperatureProbe1: this.recordTimeComplex.bind(this),
            temperatureProbe2: this.recordTimeComplex.bind(this),
            alarm: this.recordInterval.bind(this),
        };
    }

    async init() {
        this.mongoClient = await MongoClient.connect(this.databaseOpts.url);
        this.db = this.mongoClient.db(`db-test`);
        this.timeComplexColl = this.db.collection(`timeComplex`);
        this.intervalColl = this.db.collection(`interval`);
    }

    async cleanup() {
        this.mongoClient.close();
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
