"use strict";

const cassandra = require("cassandra-driver");
const FlattenJS = require("flattenjs");
const _ = require("lodash");
const StreamConcat = require("stream-concat");

const BaseSink = require("../base/BaseSink");

const INTERVAL_BUCKET_TIME = 60000;
const MAX_LOOK_BACK = 31536000000;

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

function chooseBucketTime(interval) {
  if (interval <= 1000)         // 1 second
    return 43200000;            // 12 hours
  else if (interval <= 30000)   // 30 seconds
    return 604800000;           // 1 week
  else if (interval <= 900000)  // 15 minutes
    return 7776000000;          // 3 months
  else
    return 31536000000;         // 1 year
}

function forwardError(srcStream, dstStream) {
  srcStream.on("error", (err) => {
    dstStream.destroy(err);
  });

  return srcStream;
}

module.exports = class CassandraMachineSink extends BaseSink {

  constructor(databaseOpts) {
    super(databaseOpts, 16, 4);
  }

  async init() {
    this.databaseOpts.profiles = [
      new cassandra.ExecutionProfile("default", {
        consistency: 1,
        readTimeout: 10000
      })
    ];

    this.cassandraClient = new cassandra.Client(this.databaseOpts);
    await this.cassandraClient.execute("USE db_test;", [], {});

    super.init();
  }

  async cleanup() {
    super.cleanup();

    await this.cassandraClient.shutdown();
  }

  async query(name, type, options, interval) {
    switch (type) {
    case "INTERVAL_RANGE":
      return await this.queryIntervalRange(name, options, interval);
    case "TIME_COMPLEX_RANGE":
      return await  this.queryTimeComplexRange(name, options, interval);
    case "TIME_COMPLEX_RANGE_BUCKET_AVG":
      return await this.queryTimeComplexRangeBucketAvg(name, options, interval);
    case "TIME_COMPLEX_DIFFERENCE":
      return await this.queryTimeComplexDifference(name, options, interval);
    case "TIME_COMPLEX_LAST_BEFORE":
      return await this.queryTimeComplexLastBefore(name, options, interval);
    }
  }

  async queryIntervalRange(name, options, interval) {
    let bucketTime = chooseBucketTime(interval);

    let startTime = new Date(Math.floor(options.startTime / INTERVAL_BUCKET_TIME) * INTERVAL_BUCKET_TIME);
    let endTime = new Date(Math.floor(options.endTime / INTERVAL_BUCKET_TIME) * INTERVAL_BUCKET_TIME);

    let time = Math.floor(options.startTime.getTime() / bucketTime) * bucketTime;
    let openIntervals = false;
    let self = this;
    let nextStream = function() {
      if (time >= (Math.floor(options.endTime.getTime() / bucketTime) * bucketTime + bucketTime)) {
        if (openIntervals) {
          return null;
        }
        else {
          openIntervals = true;
        }
      }

      if (openIntervals) {
        return forwardError(self.cassandraClient.stream("SELECT id, start_time, value FROM interval_open WHERE device_type = ? AND group = ? AND device = ? AND start_time <= ?", [
          options.deviceType,
          options.group,
          options.device,
          options.endTime
        ], {
          prepare: true
        }), this);
      }

      let currentTime = new Date(time);
      time += bucketTime;
     
      return forwardError(self.cassandraClient.stream("SELECT id, start_time, end_time, value FROM interval_closed WHERE device_type = ? AND group = ? AND device = ? AND bucket = ? AND interval_bucket >= ? AND interval_bucket <= ?", [
        options.deviceType,
        options.group,
        options.device,
        currentTime,
        startTime,
        endTime
      ], {
        prepare: true
      }), this);
    };

    let combinedStream = new StreamConcat(nextStream, {
      objectMode: true,
      advanceOnClose: true
    });

    let intervals = {};
    let count = 0;
    return await new Promise((resolve, reject) => {
      combinedStream.on("data", function (row) {
        if (row.start_time > options.endTime)
          return;

        if (row.end_time < options.startTime)
          return;

        if (intervals[row.id])
          return;

        ++count;
        row.value = JSON.parse(row.value);
        intervals[row.id] = true;
      }).on("end", function () {
        resolve(count);
      }).on("error", function (err) {
        reject(err);
      });
    });
  }

  async queryTimeComplexRange(name, options, interval) {
    let bucketTime = chooseBucketTime(interval);

    let time = Math.floor(options.startTime.getTime() / bucketTime) * bucketTime;
    let self = this;
    let nextStream = function() {
      if (time >= (Math.floor(options.endTime.getTime() / bucketTime) * bucketTime + bucketTime)) {
        return null;
      }

      let currentTime = new Date(time);
      time += bucketTime;
     
      return forwardError(self.cassandraClient.stream("SELECT timestamp, original_timestamp, value FROM time_complex WHERE device_type = ? AND group = ? AND device = ? AND bucket = ? AND timestamp >= ? AND timestamp <= ?", [
        options.deviceType,
        options.group,
        options.device,
        currentTime,
        options.startTime,
        options.endTime,
      ], {
        prepare: true
      }), this);
    };

    let combinedStream = new StreamConcat(nextStream, {
      objectMode: true,
      advanceOnClose: true
    });

    let count = 0;
    return await new Promise((resolve, reject) => {
      combinedStream.on("data", function (row) {
        ++count;

        row.value = JSON.parse(row.value);
        if (options.select && options.select.length)
          row.value = _.pick(row.value, options.select);
      }).on("end", function () {
        resolve(count);
      }).on("error", function (err) {
        reject(err);
      });
    });
  }

  async queryTimeComplexRangeBucketAvg(name, options, interval) {
    if (!options.select || options.select.length === 0)
      throw new Error("Selection is required");

    let bucketTime = chooseBucketTime(interval);

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

      for (let s of options.select)
        buckets[i][s + "_avg"] = 0;
    }

    let time = Math.floor(options.startTime.getTime() / bucketTime) * bucketTime;
    let self = this;
    let nextStream = function() {
      if (time >= (Math.floor(options.endTime.getTime() / bucketTime) * bucketTime + bucketTime)) {
        return null;
      }

      let currentTime = new Date(time);
      time += bucketTime;
     
      return forwardError(self.cassandraClient.stream("SELECT timestamp, value FROM time_complex WHERE device_type = ? AND group = ? AND device = ? AND bucket = ? AND timestamp >= ? AND timestamp <= ?", [
        options.deviceType,
        options.group,
        options.device,
        currentTime,
        options.startTime,
        options.endTime,
      ], {
        prepare: true
      }), this);
    };

    let combinedStream = new StreamConcat(nextStream, {
      objectMode: true,
      advanceOnClose: true
    });

    let count = 0;
    await new Promise((resolve, reject) => {
      combinedStream.on("data", function (row) {
        ++count;

        let bucketIndex = Math.floor((row.timestamp - options.startTime.getTime()) / bucketStep) - 1;
        let bucket = buckets[bucketIndex];

        if (!bucket)
          return;

        if (bucket.minTime === undefined)
          bucket.minTime = row.timestamp.getTime();

        if (bucket.maxTime === undefined)
          bucket.maxTime = row.timestamp.getTime();

        row.value = JSON.parse(row.value);

        bucket.minTime = Math.min(bucket.minTime, row.timestamp.getTime());
        bucket.maxTime = Math.max(bucket.maxTime, row.timestamp.getTime());
        ++bucket.count;
        for (let s of options.select)
          bucket[s + "_avg"] += _.get(row.value, s);
      }).on("end", function () {
        resolve(count);
      }).on("error", function (err) {
        reject(err);
      });
    });

    buckets.filter((b) => b.count > 0).map((b) => {
      let res = {
        deviceType: options.deviceType,
        device: options.device,
        group: options.group,
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

  async queryTimeComplexDifference(name, options, interval) {
    let bucketTime = chooseBucketTime(interval);

    let time = Math.floor(options.startTime.getTime() / bucketTime) * bucketTime;

    let promises = [];
    while (time < Math.floor(options.endTime.getTime() / bucketTime) * bucketTime + bucketTime) {
      promises.push(this.cassandraClient.execute("SELECT timestamp, value FROM time_complex WHERE device_type = ? AND group = ? AND device = ? AND bucket = ? AND timestamp >= ? AND timestamp <= ? ORDER BY timestamp ASC LIMIT 1", [
        options.deviceType,
        options.group,
        options.device,
        new Date(time),
        options.startTime,
        options.endTime,
      ], {
        prepare: true
      }));
  
      promises.push(this.cassandraClient.execute("SELECT timestamp, value FROM time_complex WHERE device_type = ? AND group = ? AND device = ? AND bucket = ? AND timestamp >= ? AND timestamp <= ? ORDER BY timestamp DESC LIMIT 1", [
        options.deviceType,
        options.group,
        options.device,
        new Date(time),
        options.startTime,
        options.endTime,
      ], {
        prepare: true
      }));

      time += bucketTime;
    }

    let rawResults = await Promise.all(promises);

    let results = [];
    for (let i = 0; i < rawResults.length; i += 2) {
      if (!results[0] && rawResults[i].rowLength)
        results[0] = rawResults[i];

      if (rawResults[i + 1].rowLength)
        results[1] = rawResults[i + 1];
    }

    if (!results[0])
      results[0] = rawResults[0];

    if (!results[1])
      results[1] = rawResults[rawResults.length - 1];

    let first = results[0].rows[0];
    let last = results[1].rows[0];

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

    subtractValues(firstValue, lastValue);

    return 1;
  }

  async queryTimeComplexLastBefore(name, options, interval) {
    let bucketTime = chooseBucketTime(interval);

    let startTime = new Date(options.time.getTime() - MAX_LOOK_BACK);
    let endTime = options.time;

    let time = Math.floor(startTime.getTime() / bucketTime) * bucketTime;

    let promises = [];
    while (time < Math.floor(endTime.getTime() / bucketTime) * bucketTime + bucketTime) {
      promises.push(this.cassandraClient.execute("SELECT timestamp, original_timestamp, value FROM time_complex WHERE device_type = ? AND group = ? AND device = ? AND bucket = ? AND timestamp <= ? ORDER BY timestamp DESC LIMIT 1", [
        options.deviceType,
        options.group,
        options.device,
        new Date(time),
        endTime,
      ], {
        prepare: true
      }));

      time += bucketTime;
    }

    let rawResults = await Promise.all(promises);

    let count = 0;
    for (let i = rawResults.length - 1; i >= 0; --i) {
      if (rawResults[i].rowLength)
        ++count;
    }

    return count;
  }

  async record(id, groupName, sample, interval) {
    switch (sample.type) {
    case "TIME_COMPLEX":
      return await this.recordTimeComplex(id, groupName, sample.value, interval);
    case "INTERVAL":
      return await this.recordInterval(id, groupName, sample.value, interval);
    }
  }

  async recordTimeComplex(id, groupName, sample, interval) {
    let bucketTime = chooseBucketTime(interval);

    let bucket = new Date(Math.floor(sample.time / bucketTime) * bucketTime);

    await this.cassandraClient.execute("INSERT INTO time_complex (device_type, group, device, bucket, timestamp, original_timestamp, value) VALUES (?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?", [
      sample.deviceType,
      groupName,
      sample.device,
      bucket,
      sample.time,
      sample[groupName].time,
      JSON.stringify(sample[groupName].value),
      sample.time.getTime() * 1000
    ], {
      prepare: true
    });
  }

  async recordInterval(id, groupName, sample, interval) {
    let cql = "INSERT INTO interval_closed (device_type, group, device, bucket, interval_bucket, id, start_time, end_time, value) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?";

    let promises = [];
    let time = Math.floor(sample.startTime.getTime() / INTERVAL_BUCKET_TIME) * INTERVAL_BUCKET_TIME;
    while (time < (sample.endTime.getTime() + INTERVAL_BUCKET_TIME)) {
      let bucketTime = chooseBucketTime(interval);

      let bucket = new Date(Math.floor(time / bucketTime) * bucketTime);

      promises.push(this.cassandraClient.execute(cql, [
        sample.deviceType,
        groupName,
        sample.device,
        bucket,
        new Date(time),
        id,
        sample.startTime,
        sample.endTime,
        JSON.stringify(sample.value),
        sample.endTime.getTime() * 1000
      ], {
        prepare: true
      }));

      time += INTERVAL_BUCKET_TIME;
    }

    await Promise.all(promises);
  }

};