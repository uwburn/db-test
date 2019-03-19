"use strict";

const couchbase = require("couchbase");
const _ = require("lodash");
const FlattenJS = require("flattenjs");
const bluebird = require("bluebird");

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

module.exports = class CouchbaseMachineSink extends BaseSink {

  constructor(databaseOpts) {
    super(databaseOpts, 16, 4);
  }

  async init() {
    this.couchbaseCluster = new couchbase.Cluster(this.databaseOpts.url);
    this.couchbaseCluster.authenticate(this.databaseOpts.username, this.databaseOpts.password);
    bluebird.promisifyAll(this.couchbaseCluster);

    this.couchbaseBucket = await new Promise((resolve) => {
      let bucket = this.couchbaseCluster.openBucket("db-test");
      bluebird.promisifyAll(bucket);
      bucket.once("connect", () => resolve(bucket));
      bucket.on("error", (err) => console.log(err));
    });

    super.init();
  }

  async cleanup() {
    super.cleanup();

    this.couchbaseBucket.disconnect();
  }

  async query(name, type, options, interval) {
    switch (type) {
    case "INTERVAL_RANGE":
      return await this.queryIntervalRange(name, options);
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

  async queryIntervalRange(name, options) {
    let q = couchbase.N1qlQuery.fromString("SELECT * FROM `db-test` WHERE type = \"interval\" AND `group` = $group AND device = $device AND startTime <= $endTime AND endTime >= $startTime");
    let req = await this.couchbaseBucket.query(q, {
      group: options.group,
      device: options.device,
      startTime: options.startTime.getTime(),
      endTime: options.endTime.getTime()
    });  

    return await new Promise((resolve, reject) => {
      let count = 0;

      req.on("row", (row) => {
        if (row)
          count++;
      });
      req.on("error", (err) => {
        reject(err);
      });
      req.on("end", () => {
        resolve(count);
      });
    });
  }

  async queryTimeComplexRange(name, options) {
    let q = couchbase.ViewQuery.from("time_complex", "by_group_device_time")
      .range([ 
        options.group, 
        options.device, 
        options.startTime.getTime()
      ], [
        options.group, 
        options.device, 
        options.endTime.getTime()
      ], true);
    let req = await this.couchbaseBucket.query(q);  

    return await new Promise((resolve, reject) => {
      let count = 0;

      req.on("row", (row) => {
        if (row)
          count++;
      });
      req.on("error", (err) => {
        reject(err);
      });
      req.on("end", () => {
        resolve(count);
      });
    });
  }

  async queryTimeComplexRangeBucketAvg(name, options) {
    if (!options.select || options.select.length === 0)
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

    let q = couchbase.ViewQuery.from("time_complex", "by_group_device_time")
      .range([ 
        options.group, 
        options.device, 
        options.startTime.getTime()
      ], [
        options.group, 
        options.device, 
        options.endTime.getTime()
      ], true);
    let req = await this.couchbaseBucket.query(q);  

    return await new Promise((resolve, reject) => {
      let count = 0;

      req.on("row", (row) => {
        ++count;

        let bucketIndex = Math.floor((row.value.time - options.startTime.getTime()) / bucketStep) - 1;
        let bucket = buckets[bucketIndex];

        if (!bucket)
          return;

        bucket.minTime = Math.min(bucket.minTime, row.value.time);
        bucket.maxTime = Math.max(bucket.maxTime, row.value.time);
        row.value = row.value.value;
        ++bucket.count;
        for (let s of options.select)
          bucket[s + "_avg"] += _.get(row.value, s);
      });
      req.on("error", (err) => {
        reject(err);
      });
      req.on("end", () => {
        resolve(count);
      });
    });
  }

  async queryTimeComplexDifference(name, options) {
    let promises = [];
    let qAsc = couchbase.ViewQuery.from("time_complex", "by_group_device_time")
      .range([ 
        options.group, 
        options.device, 
        options.startTime.getTime()
      ], [
        options.group, 
        options.device, 
        options.endTime.getTime()
      ], true)
      .order(couchbase.ViewQuery.Order.ASCENDING)
      .limit(1);

    promises.push(this.couchbaseBucket.queryAsync(qAsc));

    let qDesc = couchbase.ViewQuery.from("time_complex", "by_group_device_time")
      .range([
        options.group, 
        options.device, 
        options.endTime.getTime()
      ], [ 
        options.group, 
        options.device, 
        options.startTime.getTime()
      ], true)
      .order(couchbase.ViewQuery.Order.DESCENDING)
      .limit(1);

    promises.push(this.couchbaseBucket.queryAsync(qDesc));

    let results = await Promise.all(promises);

    let subs = [];
    for (let i = 0; i < results.length -1; ++i) {
      let first;
      let firstValue;
      if (results[i][0]) {
        first = results[i][0].value;
        firstValue = first.value;
      }
      else {
        first = {};
        firstValue = {};
      }

      let last;
      let lastValue;
      if (results[i + 1][0]) {
        last = results[i + 1][0].value;
        lastValue = last.value;
      }
      else {
        last = {};
        lastValue = {};
      }

      let sub = {
        device: options.device,
        group: options.group,
        startTime: first.time,
        endTime: last.time,
        value: subtractValues(firstValue, lastValue)
      };

      subs.push(sub);
    }

    return subs.length;
  }

  async queryTimeComplexLastBefore(name, options) {
    let q = couchbase.ViewQuery.from("time_complex", "by_group_device_time")
      .range([
        options.group, 
        options.device, 
        options.time.getTime()
      ], [
        options.group, 
        options.device
      ])
      .order(couchbase.ViewQuery.Order.DESCENDING)
      .limit(1);

    let req = await this.couchbaseBucket.query(q); 

    return await new Promise((resolve, reject) => {
      let count = 0;

      req.on("row", (row) => {
        if (row)
          count++;
      });
      req.on("error", (err) => {
        reject(err);
      });
      req.on("end", () => {
        resolve(count);
      });
    });
  }

  async record(id, groupName, sample, interval) {
    switch(sample.type) {
    case "TIME_COMPLEX":
      return await this.recordTimeComplex(id, groupName, sample.value, interval);
    case "INTERVAL":
      return await this.recordInterval(id, groupName, sample.value);
    }
  }

  async recordTimeComplex(id, groupName, sample) {
    let key = `time_complex::${groupName}::${sample.device}::${sample.time}`;

    await this.couchbaseBucket.upsertAsync(key, {
      type: "time_complex",
      deviceType: sample.deviceType,
      group: groupName,
      device: sample.device,
      time: sample.time.getTime(),
      value: sample[groupName].value
    });
  }

  async recordInterval(id, groupName, sample) {
    let key = `interval::${groupName}::${sample.device}::${sample.id}`;

    await this.couchbaseBucket.upsertAsync(key, {
      type: "interval",
      deviceType: sample.deviceType,
      group: groupName,
      device: sample.device,
      id: sample.id,
      startTime: sample.startTime.getTime(),
      endTime: sample.endTime.getTime(),
      value: sample.value
    });
  }

};