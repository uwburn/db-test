"use strict";

const { Writable } = require('stream');

const MongoClient = require('mongodb').MongoClient;
const Binary = require('mongodb').Binary;
const uuidParse = require('uuid-parse');

const HIGH_WATERMARK = 512;

module.exports = class MongoMachineData {

  constructor(databaseOpts) {
    this.databaseOpts = databaseOpts;

    this.reads = 0;
    this.successfulReads = 0;
    this.totalReadLatency = 0;
    this.writes = 0;
    this.successfulWrites = 0;
    this.totalWriteLatency = 0;
    this.errors = 0;
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

  recordStream() {
    let ws = Writable({
      objectMode: true,
      highWaterMark: HIGH_WATERMARK
    });

    ws._write = (chunk, enc, callback) => {
      let t0 = Date.now();
      this.record(chunk.id, chunk.groupName, chunk.sample).then(() => {
        ++this.successfulWrites;
        this.totalWriteLatency += Date.now() - t0;
      }).catch((err) => {
        console.error(err);
        ++this.errors;
      }).then(() => {
        ++this.writes;
      }).then(callback);
    };

    return ws;
  }

  async record(id, groupName, sample) {
    switch(sample.type) {
      case "TIME_COMPLEX":
        return await this.recordTimeComplex(id, groupName, sample.value);
      case "INTERVAL":
        return await this.recordInterval(id, groupName, sample.value);
    }
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

    await this.timeComplexColl.updateOne(criteria, update, options);
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

    await this.intervalColl.updateOne(criteria, update, options);
  }

}