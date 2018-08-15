"use strict";

const { Writable } = require('stream');

const cassandra = require('cassandra-driver');
const FlattenJS = require('flattenjs');

const HIGH_WATERMARK = 256;
const TIME_COMPLEX_MODE = process.env.CASSANDRA_TIME_COMPLEX_MODE || "BOTH";

module.exports = class CassandraMachineData {

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

}