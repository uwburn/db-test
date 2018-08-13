"use strict";

const cassandra = require('cassandra-driver');
const FlattenJS = require('flattenjs');

const BaseBulkMachineData = require("../base/BulkMachineData");

const TIME_COMPLEX_MODE = process.env.CASSANDRA_TIME_COMPLEX_MODE || "BOTH";

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
    this.cassandraClient = new cassandra.Client(this.databaseOpts);
    await this.cassandraClient.execute("USE db_test;", [], {});
  }

  async cleanup() {
    await this.cassandraClient.shutdown();
  }

  async record(id, groupName, sample) {
    return this.recordMethods[groupName](id, groupName, sample);
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
