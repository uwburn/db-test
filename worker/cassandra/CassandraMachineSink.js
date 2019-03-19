"use strict";

const cassandra = require("cassandra-driver");
const FlattenJS = require("flattenjs");
const _ = require("lodash");

const BaseSink = require("../base/BaseSink");

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

  async train(group, type, interval, sample) {
    switch(type) {
    case "TIME_COMPLEX":
      return await this.trainTimeComplex(group, type, interval, sample);
    case "INTERVAL":
      return await this.trainInterval(group, type, interval, sample);
    }
  }

  async trainTimeComplex(group, type, interval, sample) {
    let cql = `CREATE TABLE IF NOT EXISTS ${group}_time_complex (device_type text, group text, device text, timestamp timestamp, original_timestamp timestamp`;

    let flatObj;
    switch(typeof sample[group].value) {
    default:
      flatObj = FlattenJS.convert(sample[group].value);
      for (let k in flatObj) {
        cql += `, ${k}`;
        switch (typeof flatObj[k]) {
        default:
        case "string":
          cql += " text";
          break;
        case "number":
          cql += " decimal";
          break;
        case "boolean":
          cql += " boolean";
          break;
        }
      }
      break;
    case "string":
      cql += ", value text";
      break;
    case "number":
      cql += ", value decimal";
      break;
    case "boolean":
      cql += ", value boolean";
      break;
    }

    cql += ", PRIMARY KEY (( device_type, group, device ), timestamp))";

    await this.cassandraClient.execute(cql, [], {});
  }

  async trainInterval(group, type, interval, sample) {
    let cql = `CREATE TABLE IF NOT EXISTS ${group}_interval (device_type text, group text, device text, start_time timestamp, end_time timestamp`;

    let flatObj;
    switch(typeof sample.value) {
    default:
      flatObj = FlattenJS.convert(sample.value);
      for (let k in flatObj) {
        cql += `, ${k}`;
        switch (typeof flatObj[k]) {
        default:
        case "string":
          cql += " text";
          break;
        case "number":
          cql += " decimal";
          break;
        case "boolean":
          cql += " boolean";
          break;
        }
      }
      break;
    case "string":
      cql += ", value text";
      break;
    case "number":
      cql += ", value decimal";
      break;
    case "boolean":
      cql += ", value boolean";
      break;
    }

    cql += ", PRIMARY KEY (device_type, group, device, start_time, end_time))";

    await this.cassandraClient.execute(cql, [], {});
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
    }
  }

  async queryIntervalRange(name, options) {
    let count = 0;
    return await new Promise((resolve, reject) => {
      this.cassandraClient.stream(`SELECT * FROM ${options.group}_interval WHERE device_type = ? AND group = ? AND device = ? AND start_time <= ? AND end_time >= ? ALLOW FILTERING`, [
        options.deviceType,
        options.group,
        options.device,
        options.endTime,
        options.startTime
      ], {
        prepare: true
      }).on("data", function (row) {
        if (row)
          ++count;
      }).on("end", function () {
        resolve(count);
      }).on("error", function (err) {
        reject(err);
      });
    });
  }

  async queryTimeComplexRange(name, options) {
    let select;
    if (options.select && options.select.length)
      select = options.select.join(", ");
    else
      select = "*";

    let count = 0;
    return await new Promise((resolve, reject) => {
      this.cassandraClient.stream(`SELECT ${select} FROM ${options.group}_time_complex WHERE device_type = ? AND group = ? AND device = ? AND timestamp >= ? AND timestamp <= ?`, [
        options.deviceType,
        options.group,
        options.device,
        options.startTime,
        options.endTime,
      ], {
        prepare: true
      }).on("data", function (row) {
        if (row)
          ++count;
      }).on("end", function () {
        resolve(count);
      }).on("error", function (err) {
        reject(err);
      });
    });
  }

  async queryTimeComplexRangeBucketAvg(name, options) {
    let select;
    if (options.select && options.select.length)
      select = options.select.join(", ");
    else
      throw new Error("Selection is required");

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

    let count = 0;
    await new Promise((resolve, reject) => {
      this.cassandraClient.stream(`SELECT ${select} FROM ${options.group}_time_complex WHERE device_type = ? AND group = ? AND device = ? AND timestamp >= ? AND timestamp <= ?`, [
        options.deviceType,
        options.group,
        options.device,
        options.startTime,
        options.endTime,
      ], {
        prepare: true
      }).on("data", function (row) {
        ++count;

        let bucketIndex = Math.floor((row.timestamp - options.startTime.getTime()) / bucketStep) - 1;
        let bucket = buckets[bucketIndex];

        if (!bucket)
          return;

        if (bucket.minTime === undefined)
          bucket.minTime = row.timestamp.getTime();

        if (bucket.maxTime === undefined)
          bucket.maxTime = row.timestamp.getTime();

        bucket.minTime = Math.min(bucket.minTime, row.timestamp.getTime());
        bucket.maxTime = Math.max(bucket.maxTime, row.timestamp.getTime());
        ++bucket.count;
        for (let s of options.select)
          bucket[s + "_avg"] += row.value[s];
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

  async queryTimeComplexDifference(name, options) {
    let promises = [];

    promises.push(this.cassandraClient.execute(`SELECT * FROM ${options.group}_time_complex WHERE device_type = ? AND group = ? AND device = ? AND timestamp >= ? AND timestamp <= ? ORDER BY timestamp ASC LIMIT 1`, [
      options.deviceType,
      options.group,
      options.device,
      options.startTime,
      options.endTime,
    ], {
      prepare: true
    }));

    promises.push(this.cassandraClient.execute(`SELECT * FROM ${options.group}_time_complex WHERE device_type = ? AND group = ? AND device = ? AND timestamp >= ? AND timestamp <= ? ORDER BY timestamp DESC LIMIT 1`, [
      options.deviceType,
      options.group,
      options.device,
      options.startTime,
      options.endTime,
    ], {
      prepare: true
    }));

    let results = await Promise.all(promises);

    let subs = [];
    for (let i = 0; i < results.length -1; ++i) {
      let first = results[i].rows[0];
      let last = results[i + 1].rows[0];

      if (!first)
        first = {};

      if (!last)
        last = {};

      let sub = {
        deviceType: first.device_type,
        device: first.device,
        group: first.group,
        startTime: first.timestamp,
        endTime: last.timestamp,
        value: subtractValues(first, last)
      };

      subs.push(sub);
    }

    return subs.length;
  }

  async queryTimeComplexLastBefore(name, options) {
    let count = 0;
    return await new Promise((resolve, reject) => {
      this.cassandraClient.stream(`SELECT * FROM ${options.group}_time_complex WHERE device_type = ? AND group = ? AND device = ? AND timestamp <= ? ORDER BY timestamp DESC LIMIT 1`, [
        options.deviceType,
        options.group,
        options.device,
        options.time,
      ], {
        prepare: true
      }).on("data", function (row) {
        if (row)
          ++count;
      }).on("end", function () {
        resolve(count);
      }).on("error", function (err) {
        reject(err);
      });
    });
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
    let cql1 = `INSERT INTO ${groupName}_time_complex (device_type, group, device, timestamp, original_timestamp`;
    let cql2 = ") VALUES (?, ?, ?, ?, ?";
    let params = [
      sample.deviceType,
      groupName,
      id,
      sample.time,
      sample[groupName].time
    ];

    let flatObj;
    switch(typeof sample[groupName].value) {
    default:
      flatObj = FlattenJS.convert(sample[groupName].value);
      for (let k in flatObj) {
        cql1 += `, ${k}`;
        cql2 += ", ?";
        params.push(flatObj[k]);
      }
      break;
    case "string":
    case "number":
    case "boolean":
      cql1 += ", value";
      cql2 += ", ?";
      params.push(sample[groupName].value);
      break;
    }

    let cql = cql1 + cql2 + ")";

    await this.cassandraClient.execute(cql, params, {
      prepare: true
    });
  }

  async recordInterval(id, groupName, sample) {
    let cql1 = `INSERT INTO ${groupName}_interval (device_type,  group, device, start_time, end_time`;
    let cql2 = ") VALUES (?, ?, ?, ?, ?";
    let params = [
      sample.deviceType,
      id,
      groupName,
      sample.startTime,
      sample.endTime
    ];

    let flatObj;
    switch(typeof sample.value) {
    default:
      flatObj = FlattenJS.convert(sample.value);
      for (let k in flatObj) {
        cql1 += `, ${k}`;
        cql2 += ", ?";
        params.push(flatObj[k]);
      }
      break;
    case "string":
    case "number":
    case "boolean":
      cql1 += ", value";
      cql2 += ", ?";
      params.push(sample.value);
      break;
    }

    let cql = cql1 + cql2 + ")";

    await this.cassandraClient.execute(cql, params, {
      prepare: true
    });
  }

};