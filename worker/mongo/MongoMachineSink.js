"use strict";

const { Writable } = require('stream');

const MongoClient = require('mongodb').MongoClient;
const Binary = require('mongodb').Binary;
const uuidParse = require('uuid-parse');
const _ = require('lodash');
const FlattenJS = require('flattenjs');

const HIGH_WATERMARK = 256;

function subtractDocs(o1, o2) {
  let d = {
    _id: o2._id
  };

  if (o1)
    delete o1._id;
  delete o2._id;
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

module.exports = class MongoMachineSink {

  constructor(databaseOpts) {
    this.databaseOpts = databaseOpts;
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
    let criteria = {
      device: Binary(uuidParse.parse(options.device, Buffer.allocUnsafe(16)), Binary.SUBTYPE_UUID),
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
      "_id.device": Binary(uuidParse.parse(options.device, Buffer.allocUnsafe(16)), Binary.SUBTYPE_UUID),
      "_id.time": {
        $gt: options.startTime,
        $lt: options.endTime
      }
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

      let cursor = this.timeComplexColl.find(criteria).project(project).forEach((doc) => {
        ++count;
      }, (err) => {
        if (err)
          return reject();

        resolve(count);
      });
    });
  }

  async queryTimeComplexRangeBucketAvg(name, options) {
    options.select = options.select || {};

    let criteria = {
      "_id.device": Binary(uuidParse.parse(options.device, Buffer.allocUnsafe(16)), Binary.SUBTYPE_UUID),
      "_id.time": {
        $gt: options.startTime,
        $lt: options.endTime
      }
    };

    options.groups.forEach((e) => {
      criteria[e] = { $exists: true };
    });

    let output = {
      count: { $sum: 1 }
    };
    for (let k in options.select) {
      let group = options.select[k];
      for (let path of group)
        output[k + "_" + path] =  { $avg: "$" + k + ".value." + path};
    }

    let stages = [
      {
        $match: criteria
      },
      {
        $bucketAuto: {
          groupBy: "$_id.time",
          buckets: options.buckets,
          output: output
        }
      }
    ];

    return await new Promise((resolve, reject) => {
      let count = 0;

      this.timeComplexColl.aggregate(stages).forEach((doc) => {
        ++count;
      }, (err) => {
        if (err)
          return reject();

        resolve(count);
      });
    });
  }

  async queryTimeComplexDifference(name, options) {
    options.select = options.select || {};

    let promises = [];

    for (let time of options.times) {
      let criteria = {
        "_id.device": Binary(uuidParse.parse(options.device, Buffer.allocUnsafe(16)), Binary.SUBTYPE_UUID),
        "_id.time": {
          $lt: time
        }
      };

      options.groups.forEach((e) => {
        criteria[e] = {$exists: true};
      });

      let group = {
        _id: "$_id.device"
      };
      for (let k in options.select) {
        let _group = options.select[k];

        let hasPath = false;
        for (let path of _group) {
          hasPath = true;
          group[k + "_" + path] = {$last: "$" + k + ".value." + path};
        }

        if (!hasPath) {
          group[k] = {$last: "$" + k + ".value"};
        }
      }

      let stages = [
        {
          $match: criteria
        },
        {
          $sort: {
            "_id.device": 1,
            "_id.time": 1
          }
        },
        {
          $group: group
        }
      ];

      promises.push(this.timeComplexColl.aggregate(stages).toArray());
    }

    let results = await Promise.all(promises);

    let subs = [];
    for (let i = 0; i < results.length - 1; ++i) {
      let intervalSubs = [];
      subs.push(intervalSubs);

      let prev = results[i];
      let curr = results[i + 1];

      for (let j = 0; j < curr.length; ++j)
        intervalSubs.push(subtractDocs(prev[j], curr[j]));
    }
  }

  async queryTimeComplexLastBefore(name, options) {
    let criteria = {
      "_id.device": Binary(uuidParse.parse(options.device, Buffer.allocUnsafe(16)), Binary.SUBTYPE_UUID),
      "_id.time": {
        $lt: options.time
      }
    };

    options.groups.forEach((e) => {
      criteria[e] = { $exists: true };
    });

    let group = {
      _id: "last"
    };
    for (let k in options.select) {
      let _group = options.select[k];

      let hasPath = false;
      for (let path of _group) {
        hasPath = true;
        group[k + "_" + path] = {$last: "$" + k + ".value." + path};
      }

      if (!hasPath) {
        group[k] = {$last: "$" + k + ".value"};
      }
    }

    let stages = [
      {
        $match: criteria
      },
      {
        $sort: {
          "_id.time": 1
        }
      },
      {
        $group: group
      }
    ];

    return await new Promise((resolve, reject) => {
      let count = 0;

      this.timeComplexColl.aggregate(stages).forEach((doc) => {
        ++count;
      }, (err) => {
        if (err)
          return reject();

        resolve(count);
      });
    });
  }

  async queryTimeComplexTopDifference(name, options) {
    options.select = options.select || {};
    let times = [options.startTime, options.endTime];

    let promises = [];

    for (let time of times) {
      let criteria = {
        "_id.time": {
          $lt: time
        }
      };

      options.groups.forEach((e) => {
        criteria[e] = {$exists: true};
      });

      let group = {
        _id: "$_id.device"
      };
      for (let k in options.select) {
        let _group = options.select[k];

        let hasPath = false;
        for (let path of _group) {
          hasPath = true;
          group[k + "_" + path] = {$last: "$" + k + ".value." + path};
        }

        if (!hasPath) {
          group[k] = {$last: "$" + k + ".value"};
        }
      }

      let stages = [
        {
          $match: criteria
        },
        {
          $sort: {
            "_id.device": 1,
            "_id.time": 1
          }
        },
        {
          $group: group
        }
      ];

      promises.push(this.timeComplexColl.aggregate(stages).toArray());
    }

    let results = await Promise.all(promises);

    let subs = [];
    for (let i = 0; i < results.length - 1; ++i) {
      let intervalSubs = [];
      subs.push(intervalSubs);

      let prev = results[i];
      let curr = results[i + 1];

      for (let j = 0; j < curr.length; ++j)
        intervalSubs.push(subtractDocs(prev[j], curr[j]));
    }

    let iteratees = [];
    let orders = [];
    for (let k in options.sort) {
      let group = options.sort[k];

      for (let path in group) {
        iteratees.push(k + "." + path);
        orders.push(group[path]);
      }
    }

    let tops = _.orderBy(subs, iteratees, orders);
    tops = tops.slice(0, options.limit);

    return tops.length;
  }

  async queryIntervalTopCount(name, options) {
    options.select = options.select || {};

    let criteria = {
      "_id.time": {
        startTime: { $lt: options.endTime },
        endTime: { $gt: options.startTime }
      },
      group: { $in: options.groups },
    };

    let group = {
      _id: "$_id.device",
      count: { $sum: 1 }
    };

    let sort = {
      count: -1
    };

    let stages = [
      {
        $match: criteria
      },
      {
        $group: group
      },
      {
        $sort: sort
      },
      {
        $limit : options.limit
      }
    ];

    return await new Promise((resolve, reject) => {
      let count = 0;

      this.intervalColl.aggregate(stages).forEach((doc) => {
        ++count;
      }, (err) => {
        if (err)
          return reject();

        resolve(count);
      });
    });
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