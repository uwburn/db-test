"use strict";

const MongoClient = require('mongodb').MongoClient;
const Binary = require('mongodb').Binary;
const uuidParse = require('uuid-parse');

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
      _id: {
        device: Binary(uuidParse.parse(sample.device, Buffer.allocUnsafe(16)), Binary.SUBTYPE_UUID),
        time: sample.time
      }
    };

    let update = {
      $setOnInsert: {
        deviceType: Binary(uuidParse.parse(sample.deviceType, Buffer.allocUnsafe(16)), Binary.SUBTYPE_UUID),
      },
      $set: {}
    };

    update.$set[groupName] = sample[groupName];

    let options = {
      upsert: true
    };

    await this.timeComplexColl.update(criteria, update, options);
  }

  async recordInterval(id, groupName, sample) {
    let criteria = {
      _id: Binary(uuidParse.parse(sample.id, Buffer.allocUnsafe(16)), Binary.SUBTYPE_UUID),
    };

    let update = {
      $setOnInsert: {
        deviceType: Binary(uuidParse.parse(sample.deviceType, Buffer.allocUnsafe(16)), Binary.SUBTYPE_UUID),
        device: Binary(uuidParse.parse(sample.device, Buffer.allocUnsafe(16)), Binary.SUBTYPE_UUID),
        startTime: sample.startTime,
        endTime: sample.endTime,
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
