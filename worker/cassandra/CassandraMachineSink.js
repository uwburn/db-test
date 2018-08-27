"use strict";

const { Writable } = require('stream');

const cassandra = require('cassandra-driver');
const FlattenJS = require('flattenjs');
const _ = require('lodash');

const HIGH_WATERMARK = 256;
const TIME_COMPLEX_MODE = process.env.CASSANDRA_TIME_COMPLEX_MODE || "BOTH";

const maxTime = 8640000000000000;
const maxDate = new Date(maxTime);

function subtractValues(o1, o2) {
  let d = { };

  let f1 = FlattenJS.convert(o1);
  let f2 = FlattenJS.convert(o2);

  for (let k in f2) {
    if (isNaN(f2[k]))
      continue;

    if (isNaN(f1[k])) {
      d[k] = f2[k];
      continue;
    }

    d[k] = f2[k] - f1[k];
  }

  let res = {};
  for (let k in d)
    _.set(res, k, d[k]);

  return res;
}

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
    options.select = options.select || {};

    if (options.groups.length === 1)
      return await this.queryIntervalRangeSingleGroup(name, options);
    else
      return await this.queryIntervalRangeMultipleGroups(name, options);
  }

  async queryIntervalRangeSingleGroup(name, options) {
    let group = options.groups[0];

    let count = 0;
    return await new Promise((resolve, reject) => {
      this.cassandraClient.stream("SELECT * FROM interval WHERE device_type = ? AND group = ? AND device = ? AND (start_time, end_time) >= (?, ?) AND (start_time, end_time) <= (?, ?)", [
        options.deviceType,
        group,
        options.device,
        options.startTime,
        options.startTime,
        options.endTime,
        maxDate,
      ], {
        prepare: true
      }).on('data', function (row) {
        ++count;
        row.value = JSON.parse(row.value);
      }).on('end', function () {
        resolve(count);
      }).on('error', function (err) {
        reject(err);
      });
    });
  }

  async queryIntervalRangeMultipleGroups(name, options) {
    throw new Error("Currently not supported, as no query requires it");
  }

  async queryTimeComplexRange(name, options) {
    options.select = options.select || {};

    if (options.groups.length === 1)
      return await this.queryTimeComplexRangeSingleGroup(name, options);
    else
      return await this.queryTimeComplexRangeMultiGroups(name, options);
  }

  async queryTimeComplexRangeSingleGroup(name, options) {
    let group = options.groups[0];
    let paths = options.select[group];

    let count = 0;
    return await new Promise((resolve, reject) => {
      this.cassandraClient.stream("SELECT * FROM time_complex WHERE device_type = ? AND group = ? AND device = ? AND timestamp >= ? AND timestamp <= ?", [
        options.deviceType,
        group,
        options.device,
        options.startTime,
        options.endTime,
      ], {
        prepare: true
      }).on('data', function (row) {
        ++count;

        row.value = JSON.parse(row.value);
        row.value = _.pick(row.value, paths);
      }).on('end', function () {
        resolve(count);
      }).on('error', function (err) {
        reject(err);
      });
    });
  }

  async queryTimeComplexRangeMultiGroups(name, options) {
    throw new Error("Currently not supported, as no query requires it");
  }

  async queryTimeComplexRangeBucketAvg(name, options) {
    options.select = options.select || {};

    if (options.groups.length === 1)
      return await this.queryTimeComplexRangeBucketAvgSingleGroup(name, options);
    else
      return await this.queryTimeComplexRangeBucketAvgMultiGroups(name, options);
  }

  async queryTimeComplexRangeBucketAvgSingleGroup(name, options) {
    let group = options.groups[0];
    let paths = options.select[group];

    let duration = options.endTime.getTime() - options.startTime.getTime();
    let bucketStep = Math.round(duration / options.buckets);

    let buckets = [];
    for (let i = 0; i < options.buckets; ++i) {
      buckets[i] = {
        time: options.startTime.getTime() + i * bucketStep,
        count: 0,
        minTime: Number.MAX_SAFE_INTEGER,
        maxTime: Number.MIN_SAFE_INTEGER
      };

      for (let path of paths)
        buckets[i][group + "_" + path] = 0;
    }

    let count = 0;
    await new Promise((resolve, reject) => {
      this.cassandraClient.stream("SELECT * FROM time_complex WHERE device_type = ? AND group = ? AND device = ? AND timestamp >= ? AND timestamp <= ?", [
        options.deviceType,
        group,
        options.device,
        options.startTime,
        options.endTime,
      ], {
        prepare: true
      }).on('data', function (row) {
        ++count;

        let bucketIndex = Math.floor((row.timestamp - options.startTime.getTime()) / bucketStep) - 1;
        let bucket = buckets[bucketIndex];

        if (!bucket)
          return;

        row.value = JSON.parse(row.value);

        bucket.minTime = Math.min(bucket.minTime, row.time);
        bucket.maxTime = Math.max(bucket.maxTime, row.time);
        ++bucket.count;
        for (let path of paths)
          bucket[group + "_" + path + "_avg"] += _.get(row.value, path);
      }).on('end', function () {
        resolve(count);
      }).on('error', function (err) {
        reject(err);
      });
    });

    buckets.filter((b) => b.count > 0).map((b) => {
      let res = {
        deviceType: options.deviceType,
        device: options.device,
        group: group,
        time: new Date(b.time),
        count: b.count,
      };

      for (let k in b) {
        if (!k.endsWith("_avg"))
          continue;

        res[k.substring(0, k.length - 4)] = b[k] / b.count;
      }
    });

    return buckets.length;
  }

  async queryTimeComplexRangeBucketAvgMultiGroups(name, options) {
    throw new Error("Currently not supported, as no query requires it");
  }

  async queryTimeComplexDifference(name, options) {
    options.select = options.select || {};

    if (options.groups.length === 1)
      return await this.queryTimeComplexDifferenceSingleGroup(name, options);
    else
      return await this.queryTimeComplexDifferenceMultiGroups(name, options);
  }

  async queryTimeComplexDifferenceSingleGroup(name, options) {
    let group = options.groups[0];
    let paths = options.select[group];

    let promises = [];
    for (let i = 0; i < options.times.length -1; ++i) {
      let startTime = options.times[i];
      let endTime = options.times[i + 1];

      promises.push(this.cassandraClient.execute("SELECT * FROM time_complex WHERE device_type = ? AND group = ? AND device = ? AND timestamp >= ? AND timestamp <= ? ORDER BY timestamp ASC LIMIT 1", [
        options.deviceType,
        group,
        options.device,
        startTime,
        endTime,
      ], {
        prepare: true
      }));

      promises.push(this.cassandraClient.execute("SELECT * FROM time_complex WHERE device_type = ? AND group = ? AND device = ? AND timestamp >= ? AND timestamp <= ? ORDER BY timestamp DESC LIMIT 1", [
        options.deviceType,
        group,
        options.device,
        startTime,
        endTime,
      ], {
        prepare: true
      }));
    }

    let results = await Promise.all(promises);

    let subs = [];
    for (let i = 0; i < results.length -1; ++i) {
      let first = results[i].rows[0];
      let last = results[i + 1].rows[0];

      let firstValue;
      let lastValue;
      if (first) {
        firstValue = JSON.parse(first.value);
      }
      else {
        first = {};
        firstValue = {};
      }

      if (last) {
        lastValue = JSON.parse(last.value);
      }
      else {
        last = {};
        lastValue = {};
      }

      let sub = {
        deviceType: first.device_type,
        device: first.device,
        group: first.group,
        startTime: first.timestamp,
        endTime: last.timestamp,
        value: subtractValues(firstValue, lastValue)
      };

      subs.push(sub);
    }

    return subs.length;
  }

  async queryTimeComplexDifferenceMultiGroups(name, options) {
    throw new Error("Currently not supported, as no query requires it");
  }

  async queryTimeComplexLastBefore(name, options) {
    options.select = options.select || {};

    if (options.groups.length === 1)
      return await this.queryTimeComplexLastBeforeSingleGroup(name, options);
    else
      return await this.queryTimeComplexLastBeforeMultipleGroups(name, options);
  }

  async queryTimeComplexLastBeforeSingleGroup(name, options) {
    let group = options.groups[0];
    let paths = options.select[group];

    let count = 0;
    return await new Promise((resolve, reject) => {
      this.cassandraClient.stream("SELECT * FROM time_complex WHERE device_type = ? AND group = ? AND device = ? AND timestamp <= ? ORDER BY timestamp DESC LIMIT 1", [
        options.deviceType,
        group,
        options.device,
        options.time,
      ], {
        prepare: true
      }).on('data', function (row) {
        ++count;

        row.value = _.pick(row.value, paths);
      }).on('end', function () {
        resolve(count);
      }).on('error', function (err) {
        reject(err);
      });
    });
  }

  async queryTimeComplexLastBeforeMultipleGroups(name, options) {
    throw new Error("Currently not supported, as no query requires it");
  }

  async queryTimeComplexTopDifference(name, options) {
    options.select = options.select || {};

    if (options.groups.length === 1)
      return await this.queryTimeComplexTopDifferenceSingleGroup(name, options);
    else
      return await this.queryTimeComplexTopDifferenceMultipleGroups(name, options);
  }

  async queryTimeComplexTopDifferenceSingleGroup(name, options) {
    let group = options.groups[0];
    let sorts = options.sort[group];

    let boundaries = await this.cassandraClient.execute("SELECT device, MIN(timestamp) AS min_time, MAX(timestamp) AS max_time FROM time_complex WHERE device_type = ? AND group = ? AND timestamp >= ? AND timestamp <= ? GROUP BY device LIMIT 1000000 ALLOW FILTERING", [
      options.deviceType,
      group,
      options.startTime,
      options.endTime,
    ], {
      prepare: true
    });

    let promises = [];
    for (let boundary of boundaries.rows) {
      promises.push(this.cassandraClient.execute("SELECT * FROM time_complex WHERE device_type = ? AND group = ? AND device = ? AND timestamp = ?", [
        options.deviceType,
        group,
        boundary.device,
        boundary.min_time,
      ], {
        prepare: true
      }), this.cassandraClient.execute("SELECT * FROM time_complex WHERE device_type = ? AND group = ? AND device = ? AND timestamp = ?", [
        options.deviceType,
        group,
        boundary.device,
        boundary.max_time,
      ], {
        prepare: true
      }));
    }

    let results = await Promise.all(promises);

    let subs = [];
    for (let i = 0; i < results.length; i += 2) {
      let first = results[i].rows[0];
      let last = results[i + 1].rows[0];

      first.value = JSON.parse(first.value);
      last.value = JSON.parse(last.value);

      let sub = {
        deviceType: first.device_type,
        device: first.device,
        group: first.group,
        value: subtractValues(first.value, last.value)
      };

      subs.push(sub);
    }

    let iteratees = [];
    let orders = [];
    for (let path in sorts) {
      iteratees.push("value." + path);
      orders.push(group[path]);
    }

    let tops = _.orderBy(subs, iteratees, orders);
    tops = tops.slice(0, options.limit);

    return tops.length;
  }

  async queryTimeComplexTopDifferenceMultipleGroups(name, options) {
    throw new Error("Currently not supported, as no query requires it");
  }

  async queryIntervalTopCount(name, options) {
    options.select = options.select || {};

    if (options.groups.length === 1)
      return await this.queryIntervalTopCountSingleGroup(name, options);
    else
      return await this.queryIntervalTopCountMultipleGroups(name, options);
  }

  async queryIntervalTopCountSingleGroup(name, options) {
    let group = options.groups[0];

    let counts = await this.cassandraClient.execute("SELECT device, COUNT(*) AS interval_count FROM interval WHERE device_type = ? AND group = ? GROUP BY device;", [
      options.deviceType,
      group
    ], {
      prepare: true
    });

    let tops = _.orderBy(counts.rows, ["interval_count"], ["DESC"]);
    tops = tops.slice(0, options.limit);
  }

  async queryIntervalTopCountMultipleGroups(name, options) {
    throw new Error("Currently not supported, as no query requires it");
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
      promises.push(this.cassandraClient.execute("INSERT INTO time_complex (device_type, group, device, timestamp, original_timestamp, value) VALUES (?, ?, ?, ?, ?, ?)", [
        sample.deviceType,
        groupName,
        id,
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
        promises.push(this.cassandraClient.execute("INSERT INTO time_flat_complex (device_type, group, device, path, timestamp, original_timestamp, value) VALUES (?, ?, ?, ?, ?, ?, ?)", [
          sample.deviceType,
          groupName,
          id,
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