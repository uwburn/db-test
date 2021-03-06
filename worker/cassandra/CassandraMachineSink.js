"use strict";

const cassandra = require("cassandra-driver");
const FlattenJS = require("flattenjs");
const _ = require("lodash");
const avro = require("avsc");
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
    super(databaseOpts, 8, 64);
  }

  async init() {
    this.databaseOpts.queryOptions = {
      consistency: 1
    };

    this.databaseOpts.profiles = [
      new cassandra.ExecutionProfile("default", {
        consistency: 1,
        readTimeout: 10000
      })
    ];

    this.cassandraClient = new cassandra.Client(this.databaseOpts);
    await this.cassandraClient.execute("USE db_test;", [], {});

    this.avroTypes = {};

    super.init();
  }

  async cleanup() {
    super.cleanup();

    await this.cassandraClient.shutdown();
  }

  async train(group, type, interval, sample) {
    switch(type) {
    case "TIME_COMPLEX":
      return this.avroTypes[group] = avro.Type.forValue(sample[group].value);
    case "INTERVAL":
      return this.avroTypes[group] = avro.Type.forValue({
        st: sample.startTime,
        et: sample.endTime,
        v: sample.value
      });
    }
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
    let avroType = this.avroTypes[options.group];

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
        return forwardError(self.cassandraClient.stream("SELECT id, st, v FROM interval_open WHERE dt = ? AND g = ? AND d = ? AND st <= ?", [
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
     
      return forwardError(self.cassandraClient.stream("SELECT id, v FROM interval_closed WHERE dt = ? AND g = ? AND d = ? AND p = ? AND b >= ? AND b <= ?", [
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
        row.v = avroType.fromBuffer(row.v);
        if (row.v.st > options.endTime)
          return;

        if (row.v.et < options.startTime)
          return;

        if (intervals[row.id])
          return;

        ++count;
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
    let avroType = this.avroTypes[options.group];

    let time = Math.floor(options.startTime.getTime() / bucketTime) * bucketTime;
    let self = this;
    let nextStream = function() {
      if (time >= (Math.floor(options.endTime.getTime() / bucketTime) * bucketTime + bucketTime)) {
        return null;
      }

      let currentTime = new Date(time);
      time += bucketTime;
     
      return forwardError(self.cassandraClient.stream("SELECT t, v FROM time_complex WHERE dt = ? AND g = ? AND d = ? AND p = ? AND t >= ? AND t <= ?", [
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

        row.v = avroType.fromBuffer(row.v);
        if (options.select && options.select.length)
          row.v = _.pick(row.v, options.select);
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
    let avroType = this.avroTypes[options.group];

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
     
      return forwardError(self.cassandraClient.stream("SELECT t, v FROM time_complex WHERE dt = ? AND g = ? AND d = ? AND p = ? AND t >= ? AND t <= ?", [
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

        let bucketIndex = Math.floor((row.t - options.startTime.getTime()) / bucketStep);
        let bucket = buckets[bucketIndex];

        if (!bucket)
          return;

        if (bucket.minTime === undefined)
          bucket.minTime = row.t.getTime();

        if (bucket.maxTime === undefined)
          bucket.maxTime = row.t.getTime();

        row.v = avroType.fromBuffer(row.v);

        bucket.minTime = Math.min(bucket.minTime, row.t.getTime());
        bucket.maxTime = Math.max(bucket.maxTime, row.t.getTime());
        ++bucket.count;
        for (let s of options.select)
          bucket[s + "_avg"] += _.get(row.v, s);
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

    return count;
  }

  async queryTimeComplexDifference(name, options, interval) {
    let bucketTime = chooseBucketTime(interval);
    let avroType = this.avroTypes[options.group];

    let time = Math.floor(options.startTime.getTime() / bucketTime) * bucketTime;

    let promises = [];
    while (time < Math.floor(options.endTime.getTime() / bucketTime) * bucketTime + bucketTime) {
      promises.push(this.cassandraClient.execute("SELECT t, v FROM time_complex WHERE dt = ? AND g = ? AND d = ? AND p = ? AND t >= ? AND t <= ? ORDER BY t ASC LIMIT 1", [
        options.deviceType,
        options.group,
        options.device,
        new Date(time),
        options.startTime,
        options.endTime,
      ], {
        prepare: true
      }));
  
      promises.push(this.cassandraClient.execute("SELECT t, v FROM time_complex WHERE dt = ? AND g = ? AND d = ? AND p = ? AND t >= ? AND t <= ? ORDER BY t DESC LIMIT 1", [
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

    let results = await Promise.all(promises);

    let count = 0;
    let firstValue;
    let lastValue;

    if (results[0].rowLength) {
      ++count;
      firstValue = avroType.fromBuffer(results[0].rows[0].v);
    }
    else {
      firstValue = {};
    }

    if (results[1].rowLength) {
      ++count;
      lastValue = avroType.fromBuffer(results[1].rows[0].v);
    }
    else {
      lastValue = {};
    }

    subtractValues(firstValue, lastValue);

    return count;
  }

  async queryTimeComplexLastBefore(name, options, interval) {
    let bucketTime = chooseBucketTime(interval);
    let avroType = this.avroTypes[options.group];

    let startTime = new Date(options.time.getTime() - MAX_LOOK_BACK);
    let endTime = options.time;

    let time = Math.floor(startTime.getTime() / bucketTime) * bucketTime;

    let promises = [];
    while (time < Math.floor(endTime.getTime() / bucketTime) * bucketTime + bucketTime) {
      promises.push(this.cassandraClient.execute("SELECT t, v FROM time_complex WHERE dt = ? AND g = ? AND d = ? AND p = ? AND t <= ? ORDER BY t DESC LIMIT 1", [
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
      if (rawResults[i].rowLength) {
        ++count;

        if (count === 1)
          rawResults[i].rows[0].v = avroType.fromBuffer(rawResults[i].rows[0].v);
      }
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
    let avroType = this.avroTypes[groupName];

    let bucket = new Date(Math.floor(sample.time / bucketTime) * bucketTime);

    await this.cassandraClient.execute("INSERT INTO time_complex (dt, g, d, p, t, v) VALUES (?, ?, ?, ?, ?, ?) USING TIMESTAMP ?", [
      sample.deviceType,
      groupName,
      sample.device,
      bucket,
      sample.time,
      avroType.toBuffer(sample[groupName].value),
      sample.time.getTime() * 1000
    ], {
      prepare: true
    });
  }

  async recordInterval(id, groupName, sample, interval) {
    let avroType = this.avroTypes[groupName];
    let cql = "INSERT INTO interval_closed (dt, g, d, p, b, id, v) VALUES (?, ?, ?, ?, ?, ?, ?) USING TIMESTAMP ?";

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
        avroType.toBuffer({
          st: sample.startTime,
          et: sample.endTime,
          v: sample.value
        }),
        sample.endTime.getTime() * 1000
      ], {
        prepare: true
      }));

      time += INTERVAL_BUCKET_TIME;
    }

    await Promise.all(promises);
  }

};