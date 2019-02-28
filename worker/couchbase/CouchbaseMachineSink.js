"use strict";

const couchbase = require('couchbase');
const _ = require('lodash');
const FlattenJS = require('flattenjs');
const bluebird = require("bluebird");

const BaseSink = require(`../base/BaseSink`);

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
    super(databaseOpts, 16, 16);
  }

  async init() {
    this.couchbaseCluster = new couchbase.Cluster(this.databaseOpts.url);
    this.couchbaseCluster.authenticate(this.databaseOpts.username, this.databaseOpts.password);
    bluebird.promisifyAll(this.couchbaseCluster);

    this.couchbaseBucket = await new Promise((resolve, reject) => {
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
      case "TIME_COMPLEX_TOP_DIFFERENCE":
        return await this.queryTimeComplexTopDifference(name, options, interval);
      case "INTERVAL_TOP_COUNT":
        return await this.queryIntervalTopCount(name, options);
    }
  }

  async queryIntervalRange(name, options) {
    let q = couchbase.N1qlQuery.fromString('SELECT * FROM `db-test` WHERE type = "interval" AND `group` = $group AND device = $device AND startTime <= $endTime AND endTime >= $startTime');
    let req = await this.couchbaseBucket.query(q, {
      group: options.group,
      device: options.device,
      startTime: options.startTime.getTime(),
      endTime: options.endTime.getTime()
    });  

    return await new Promise((resolve, reject) => {
      let count = 0;

      req.on('row', (row) => {
        count++;
      });
      req.on('error', (err) => {
        reject(err);
      });
      req.on('end', (meta) => {
        resolve(count);
      });
    });
  }

  async queryTimeComplexRange(name, options) {
    let q = 'SELECT'; 
    if (options.select && options.select.length > 0) {
      q += " time"
      for (let s of options.select)
        q += `, \`value\`.${s}`
    }
    else {
      q += " *"
    }
    q += ' FROM `db-test` WHERE type = "time_complex" AND `group` = $group AND device = $device AND time >= $startTime AND time <= $endTime';

    q = couchbase.N1qlQuery.fromString(q);
    let req = await this.couchbaseBucket.query(q, {
      group: options.group,
      device: options.device,
      startTime: options.startTime.getTime(),
      endTime: options.endTime.getTime()
    });  

    return await new Promise((resolve, reject) => {
      let count = 0;

      req.on('row', (row) => {
        count++;
      });
      req.on('error', (err) => {
        reject(err);
      });
      req.on('end', (meta) => {
        resolve(count);
      });
    });
  }

  async queryTimeComplexRangeBucketAvg(name, options) {
    if (!options.select || options.select.length === 0)
      throw new Error("Selection is required");

    let bin = Math.floor((options.endTime.getTime() - options.startTime.getTime()) / options.buckets);

    let q = 'SELECT COUNT(*)'; 
    for (let s of options.select) {
      q += `, AVG(\`value\`.${s}) AS avg_${s}`
    }
    q += ' FROM `db-test` WHERE type = "time_complex" AND `group` = $group AND device = $device AND time <= $endTime AND time >= $startTime GROUP BY FLOOR(time / $bin)';

    q = couchbase.N1qlQuery.fromString(q);
    let req = await this.couchbaseBucket.query(q, {
      group: options.group,
      device: options.device,
      startTime: options.startTime.getTime(),
      endTime: options.endTime.getTime(),
      bin: bin
    });  

    return await new Promise((resolve, reject) => {
      let count = 0;

      req.on('row', (row) => {
        count++;
      });
      req.on('error', (err) => {
        reject(err);
      });
      req.on('end', (meta) => {
        resolve(count);
      });
    });
  }

  async queryTimeComplexDifference(name, options) {
    let qAsc = couchbase.N1qlQuery.fromString("SELECT * FROM `db-test` WHERE type = 'time_complex' AND `group` = $group AND device = $device AND time >= $startTime AND time <= $endTime ORDER BY time ASC LIMIT 1");
    let qDesc = couchbase.N1qlQuery.fromString("SELECT * FROM `db-test` WHERE type = 'time_complex' AND `group` = $group AND device = $device AND time >= $startTime AND time <= $endTime ORDER BY time DESC LIMIT 1");

    let promises = [];
    for (let i = 0; i < options.times.length -1; ++i) {
      let startTime = options.times[i];
      let endTime = options.times[i + 1];

      promises.push(this.couchbaseBucket.queryAsync(qAsc, {
        group: options.group,
        device: options.device,
        startTime: startTime.getTime(),
        endTime: endTime.getTime()
      }));

      promises.push(this.couchbaseBucket.queryAsync(qDesc, {
        group: options.group,
        device: options.device,
        startTime: startTime.getTime(),
        endTime: endTime.getTime()
      }));
    }

    let results = await Promise.all(promises);

    let subs = [];
    for (let i = 0; i < results.length -1; ++i) {
      let first;
      let firstValue;
      if (results[i][0]) {
        first = results[i][0]["db-test"];
        firstValue = first.value;
      }
      else {
        first = {};
        firstValue = {};
      }

      let last;
      let lastValue;
      if (results[i + 1][0]) {
        last = results[i + 1][0]["db-test"];
        lastValue = last.value;
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

  async queryTimeComplexLastBefore(name, options) {
    let q = couchbase.N1qlQuery.fromString('SELECT * FROM `db-test` WHERE type = "time_complex" AND `group` = $group AND device = $device AND time <= $time ORDER BY time DESC LIMIT 1');
    let req = await this.couchbaseBucket.query(q, {
      group: options.group,
      device: options.device,
      time: options.time.getTime()
    });  

    return await new Promise((resolve, reject) => {
      let count = 0;

      req.on('row', (row) => {
        count++;
      });
      req.on('error', (err) => {
        reject(err);
      });
      req.on('end', (meta) => {
        resolve(count);
      });
    });
  }

  async queryTimeComplexTopDifference(name, options, interval) {
    let q = couchbase.N1qlQuery.fromString('SELECT device, MIN(time) AS min_time, MAX(time) AS max_time FROM `db-test` WHERE type = "time_complex" AND `group` = $group AND time >= $startTime AND time <= $endTime  GROUP BY device;');
    let boundaries = await this.couchbaseBucket.queryAsync(q, {
      group: options.group,
      startTime: options.startTime.getTime(),
      endTime: options.endTime.getTime()
    });

    q = couchbase.N1qlQuery.fromString('SELECT * FROM `db-test` WHERE type = "time_complex" AND `group` = $group AND device = $device AND time = $time;');

    let promises = [];
    for (let boundary of boundaries) {
      promises.push(this.couchbaseBucket.queryAsync(q, {
        group: options.group,
        device: boundary.device,
        time: boundary.min_time
      }));

      promises.push(this.couchbaseBucket.queryAsync(q, {
        group: options.group,
        device: boundary.device,
        time: boundary.max_time
      }));
    }

    let results = await Promise.all(promises);

    let subs = [];
    for (let i = 0; i < results.length; i += 2) {
      let first = results[i][0]["db-test"];
      let last = results[i + 1][0]["db-test"];

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
    for (let s in options.sort) {
      iteratees.push("value." + s);
      orders.push(options.sort[s]);
    }

    let tops = _.orderBy(subs, iteratees, orders);
    tops = tops.slice(0, options.limit);

    return tops.length;
  }

  async queryIntervalTopCount(name, options) {
    let q = couchbase.N1qlQuery.fromString('SELECT device, COUNT(*) c FROM `db-test` WHERE type = "interval" AND `group` = $group AND startTime <= $endTime AND endTime >= $startTime GROUP BY device ORDER BY c DESC LIMIT $limit');
    let req = await this.couchbaseBucket.query(q, {
      group: options.group,
      startTime: options.startTime.getTime(),
      endTime: options.endTime.getTime(),
      limit: options.limit
    });  

    return await new Promise((resolve, reject) => {
      let count = 0;

      req.on('row', (row) => {
        count++;
      });
      req.on('error', (err) => {
        reject(err);
      });
      req.on('end', (meta) => {
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