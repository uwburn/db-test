"use strict";

const { Writable } = require('stream');

const cassandra = require('cassandra-driver');
const FlattenJS = require('flattenjs');

const HIGH_WATERMARK = 256;
const TIME_COMPLEX_MODE = process.env.CASSANDRA_TIME_COMPLEX_MODE || "BOTH";

module.exports = class CassandraMachineSink {

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
    this.cassandraClient = new cassandra.Client(this.databaseOpts);
    await this.cassandraClient.execute("USE db_test;", [], {});
  }

  async cleanup() {
    await this.cassandraClient.shutdown();
  }

  queryStream() {
    let result = {
      stream: Writable({
        objectMode: true,
        highWaterMark: HIGH_WATERMARK
      }),
      reads: 0,
      successfulReads: 0,
      totalReadLatency: 0,
      writes: 0,
      successfulWrites: 0,
      totalWriteLatency: 0,
      errors: 0
    };

    result.stream._write = (chunk, enc, callback) => {
      let t0 = Date.now();
      this.query(chunk.name, chunk.type, chunk.options).then(() => {
        ++result.successfulReads;
        result.totalReadLatency += Date.now() - t0;
      }).catch((err) => {
        console.error(err);
        ++result.errors;
      }).then(() => {
        ++result.reads;
      }).then(callback);
    };

    return result;
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

  }

  async queryTimeComplexRange(name, options) {

  }

  async queryTimeComplexRangeBucketAvg(name, options) {

  }

  async queryTimeComplexDifference(name, options) {

  }

  async queryTimeComplexLastBefore(name, options) {

  }

  async queryTimeComplexTopDifference(name, options) {

  }

  async queryIntervalTopCount(name, options) {

  }

  recordStream() {
    let result = {
      stream: Writable({
        objectMode: true,
        highWaterMark: HIGH_WATERMARK
      }),
      reads: 0,
      successfulReads: 0,
      totalReadLatency: 0,
      writes: 0,
      successfulWrites: 0,
      totalWriteLatency: 0,
      errors: 0
    };

    result.stream._write = (chunk, enc, callback) => {
      let t0 = Date.now();
      this.record(chunk.id, chunk.groupName, chunk.sample).then(() => {
        ++result.successfulWrites;
        result.totalWriteLatency += Date.now() - t0;
      }).catch((err) => {
        console.error(err);
        ++result.errors;
      }).then(() => {
        ++result.writes;
      }).then(callback);
    };

    return result;
  }

  async record(id, groupName, sample) {
    switch (sample.type) {
      case "TIME_COMPLEX":
        return await this.recordTimeComplex(id, groupName, sample.value);
      case "INTERVAL":
        return await this.recordInterval(id, groupName, sample.value);
    }
  }

  async recordTimeComplex(id, groupName, sample) {
    let promises = [];

    if (TIME_COMPLEX_MODE === "BOTH" || TIME_COMPLEX_MODE === "WHOLE") {
      promises.push(this.cassandraClient.execute("INSERT INTO time_complex (device_type, device, group, timestamp, original_timestamp, value) VALUES (?, ?, ?, ?, ?, ?)", [
        sample.deviceType,
        id,
        groupName,
        sample.time,
        sample[groupName].time,
        JSON.stringify(sample[groupName].value)
      ], {
          prepare: true
        }));
    }

    if (TIME_COMPLEX_MODE === "BOTH" || TIME_COMPLEX_MODE === "FLAT") {
      let flattened = FlattenJS.convert(sample[groupName].value);

      if (Object.keys(flattened).length === 0)
        flattened[""] = sample[groupName].value;

      for (let path in flattened) {
        promises.push(this.cassandraClient.execute("INSERT INTO time_flat_complex (device_type, device, group, path, timestamp, original_timestamp, value) VALUES (?, ?, ?, ?, ?, ?, ?)", [
          sample.deviceType,
          id,
          groupName,
          path,
          sample.time,
          sample[groupName].time,
          JSON.stringify(flattened[path])
        ], {
            prepare: true
          }));
      }
    }

    await Promise.all(promises);
  }

  async recordInterval(id, groupName, sample) {
    await this.cassandraClient.execute("INSERT INTO interval (device_type, device, group, start_time, end_time, value) VALUES (?, ?, ?, ?, ?, ?)", [
      sample.deviceType,
      id,
      groupName,
      sample.startTime,
      sample.endTime,
      JSON.stringify(sample.value)
    ], {
        prepare: true
      });
  }

};