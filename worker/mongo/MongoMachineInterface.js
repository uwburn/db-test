"use strict";

const { Writable } = require('stream');

const MongoClient = require('mongodb').MongoClient;
const Binary = require('mongodb').Binary;
const uuidParse = require('uuid-parse');

const HIGH_WATERMARK = 256;

module.exports = class MongoMachineInterface {

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
    this.mongoClient = await MongoClient.connect(this.databaseOpts.url, this.databaseOpts.options);
    this.db = this.mongoClient.db(`db-test`);
    this.timeComplexColl = this.db.collection(`timeComplex`);
    this.intervalColl = this.db.collection(`interval`);
  }

  async cleanup() {
    this.mongoClient.close();
  }

  queryStream() {
    let ws = Writable({
      objectMode: true,
      highWaterMark: HIGH_WATERMARK
    });

    ws._write = (chunk, enc, callback) => {
      let t0 = Date.now();
      this.query(chunk.name, chunk.type, chunk.options).then(() => {
        ++this.successfulReads;
        this.totalReadLatency += Date.now() - t0;
      }).catch((err) => {
        console.error(err);
        ++this.errors;
      }).then(() => {
        ++this.reads;
      }).then(callback);
    };

    return ws;
  }

  async query(name, type, options) {
    switch (type) {
      case "INTERVAL_RANGE":
        return await this.queryIntervalRange(name, options);
      case "TIME_COMPLEX_RANGE":
        return await  this.queryTimeComplexRange(name, options);
      case "TIME_COMPLEX_RANGE_BUCKET_AVG":
        return await this.queryTimeComplexRangeBucketAvg(name, options);
      case "TIME_COMPLEX_DIFFERENCE":
        return await this.queryTimeComplexDifference(name, options);
      case "TIME_COMPLEX_LAST_BEFORE":
        return await this.queryTimeComplexLastBefore(name, options);
      case "TIME_COMPLEX_TOP_DIFFERENCE":
        return await this.queryTimeComplexTopDifference(name, options);
      case "INTERVAL_TOP_COUNT":
        return await this.queryIntervalTopCount(name, options);
    }
  }

  async queryIntervalRange(name, options) {
    let criteria = {
      deviceType: options.deviceType,
      device: options.device,
      group: { $in: options.groups },
      startTime: { $lt: options.endTime },
      endTime: { $gt: options.startTime }
    };

    return await new Promise((resolve, reject) => {
      let count = 0;

      this.intervalColl.find(criteria).forEach((doc) => {
        ++count;
      }, (err) => {
        if (err)
          return reject();

        resolve(count);
      });
    });
  }

  async queryTimeComplexRange(name, options) {
    options.select = options.select || {};

    let criteria = {
      "_id.device": options.device,
      "_id.time": {
        $gt: options.startTime,
        $lt: options.endTime
      },
      deviceType: options.deviceType,
      device: options.device,
      group: { $in: options.groups },
    };

    let project = {
      "_id.time": 1,
    };

    options.groups.forEach((e) => {
      criteria[e] = { $exists: true };
      project[e] = 1;
    });

    for (let k in options.select) {
      let group = options.select[k];
      project[k + ".time"] = 1;
      delete project[k];
      for (let path of group) {
        project[k + ".value." + path] = 1;
      }
    }

    return await new Promise((resolve, reject) => {
      let count = 0;

      this.intervalColl.find(criteria).project(project).forEach((doc) => {
        ++count;
      }, (err) => {
        if (err)
          return reject();

        resolve(count);
      });
    });
  }

  async queryTimeComplexRangeBucketAvg(name, options) {
    /*options.select = options.select || {};

    let criteria = {
      "_id.device": options.device,
      "_id.time": {
        $gt: options.startTime,
        $lt: options.endTime
      },
      deviceType: options.deviceType,
      device: options.device,
      group: { $in: options.groups },
    };

    let project = {
      "_id.time": 1,
    };

    options.groups.forEach((e) => {
      criteria[e] = { $exists: true };
      project[e] = 1;
    });

    for (let k in options.select) {
      let group = options.select[k];
      project[k + ".time"] = 1;
      delete project[k];
      for (let path of group) {
        project[k + ".value." + path] = 1;
      }
    }

    let stages = [{
      $bucketAuto: {
        groupBy: "$_id.time",
        buckets: options.buckets
      }
    }];

    return await new Promise((resolve, reject) => {
      let count = 0;

      this.timeComplexColl.aggregate(stages).forEach((doc) => {
        ++count;
      }, (err) => {
        if (err)
          return reject();

        resolve(count);
      });
    });*/
  }

  queryTimeComplexDifference(name, options) {

  }

  queryTimeComplexLastBefore(name, options) {

  }

  queryTimeComplexTopDifference(name, options) {

  }

  queryIntervalTopCount(name, options) {

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
        group: groupName,
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

};