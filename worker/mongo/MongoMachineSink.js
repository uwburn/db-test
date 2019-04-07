"use strict";

const MongoClient = require("mongodb").MongoClient;
const Binary = require("mongodb").Binary;
const uuidParse = require("uuid-parse");
const _ = require("lodash");
const FlattenJS = require("flattenjs");

const BaseSink = require("../base/BaseSink");

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

function chooseBucketTime(interval) {
  if (interval <= 1000) // Second
    return 300000; // Five minutes - 300
  else if (interval <= 5000)
    return 600000; // Ten minutes - 120
  else if (interval <= 60000) // Minute
    return 3600000; // Hour - 60
  else if (interval <= 120000) // Two minutes
    return 7200000; // Two hours - 60
  else if (interval <= 300000) // Five minutes
    return 10800000; // Three hours - 36
  else if (interval <= 600000) // Ten minutes
    return 21600000; // Six hours - 36
  else if (interval <= 1800000) // Thirty minutes
    return 43200000; // Twelve hours - 24
  else if (interval <= 3600000) // Hour
    return 86400000; // Day - 24
  else if (interval <= 86400000) // Day
    return 604800000; // Week - 7
  else if (interval <= 604800000) // Week
    return 2592000000; // Month - 4
  else
    return 31536000000; // Year
}

module.exports = class MongoMachineSink extends BaseSink {

  constructor(databaseOpts) {
    super(databaseOpts, 16, 16);

    this.timeComplexCollections = { };
    this.intervalCollections = { };
  }

  async init() {
    this.mongoClient = await MongoClient.connect(this.databaseOpts.url, this.databaseOpts.options);
    this.db = this.mongoClient.db("db-test");
    this.timeComplexColl = this.db.collection("timeComplex");
    this.intervalColl = this.db.collection("interval");

    super.init();
  }

  async cleanup() {
    super.cleanup();

    this.mongoClient.close();
  }

  async train(group, type) {
    switch(type) {
    case "INTERVAL_COMPLEX":
      return await this.trainIntervalComplex();
    case "TIME_COMPLEX":
      return await this.trainTimeComplex();
    }
  }

  async trainIntervalComplex(group) {
    let collection = this.intervalCollections[group];
    if (!collection) {
      collection = this.db.collection(`${group}_interval`);
      try {
        await collection.createIndex({ device: 1 });
        await collection.createIndex({ startTime: 1 });
        await collection.createIndex({ endTime: 1 });
        await collection.createIndex({ startTime: 1, endTime: -1 });
        await collection.createIndex({ device: 1, startTime: 1, endTime: -1 });
      }
      catch(err) { /* eslint-disable-line */ }
      this.intervalCollections[group] = collection;
    }

    return collection;
  }

  async trainTimeComplex(group) {
    let collection = this.timeComplexCollections[group];
    if (!collection) {
      collection = this.db.collection(`${group}_time_complex`);
      try {
        await collection.createIndex({ "_id.device": 1 });
        await collection.createIndex({ "_id.time": 1 });
        await collection.createIndex({ "_id.device": 1, "_id.time": 1 });
        await collection.createIndex({ "_id.device": 1, "_id.time": -1 });
      }
      catch(err) { /* eslint-disable-line*/ }
      this.timeComplexCollections[group] = collection;
    }

    return collection;
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
    let coll = this.db.collection(`${options.group}_interval`);

    let criteria = {
      d: Binary(uuidParse.parse(options.device, Buffer.allocUnsafe(16)), Binary.SUBTYPE_UUID),
      st: { $lt: options.endTime },
      et: { $gt: options.startTime }
    };

    return await new Promise((resolve, reject) => {
      let count = 0;

      coll.find(criteria).forEach((doc) => {
        if (doc)
          ++count;
      }, (err) => {
        if (err)
          return reject();

        resolve(count);
      });
    });
  }

  async queryTimeComplexRange(name, options, interval) {
    let coll = this.db.collection(`${options.group}_time_complex`);

    let oStartTime = options.startTime.getTime();
    let oEndTime = options.endTime.getTime();

    let bucketTime = chooseBucketTime(interval);
    let startTime = oStartTime - (oStartTime % bucketTime);
    let endTime = oEndTime - (oEndTime % bucketTime);
    if (endTime < oEndTime)
      endTime += bucketTime;

    let stages = [
      {
        $match: {
          "_id.d": Binary(uuidParse.parse(options.device, Buffer.allocUnsafe(16)), Binary.SUBTYPE_UUID),
          "_id.t": {
            $gt: new Date(startTime),
            $lt: new Date(endTime)
          }
        }
      },
      {
        $project: {
          _id: 1,
          r: { $objectToArray: "$r" }
        }
      },
      {
        $unwind: "$r"
      },
      {
        $project: {
          t: {
            $add: [ "$_id.t", { $toInt: "$r.k" } ]
          },
          r: "$r.v"
        }
      },
      {
        $match: {
          t: {
            $gt: options.startTime,
            $lt: options.endTime
          }
        }
      }
    ];

    if (options.select) {
      let project = {
        t: 1
      };

      for (let s of options.select)
        _.set(project, `r.${s}`, 1);

      stages.push({
        $project: project
      });
    }

    return await new Promise((resolve, reject) => {
      let count = 0;

      coll.aggregate(stages, { allowDiskUse: this.databaseOpts.allowDiskUse || false }).forEach((doc) => {
        if (doc)
          count++;
      }, (err) => {
        if (err)
          return reject();

        resolve(count);
      });
    });
  }

  async queryTimeComplexRangeBucketAvg(name, options, interval) {
    if (!options.select || options.select.length === 0)
      throw new Error("Selection is required");
    
    let coll = this.db.collection(`${options.group}_time_complex`);

    let oStartTime = options.startTime.getTime();
    let oEndTime = options.endTime.getTime();

    let bucketTime = chooseBucketTime(interval);
    let startTime = oStartTime - (oStartTime % bucketTime);
    let endTime = oEndTime - (oEndTime % bucketTime);
    if (endTime < oEndTime)
      endTime += bucketTime;

    let output = {
      c: { $sum: 1 }
    };
    for (let s of options.select)
      output[s] =  { $avg: `$r.${s}` };

    let stages = [
      {
        $match: {
          "_id.d": Binary(uuidParse.parse(options.device, Buffer.allocUnsafe(16)), Binary.SUBTYPE_UUID),
          "_id.t": {
            $gt: new Date(startTime),
            $lt: new Date(endTime)
          }
        }
      },
      {
        $project: {
          _id: 1,
          r: { $objectToArray: "$r" }
        }
      },
      {
        $unwind: "$r"
      },
      {
        $project: {
          t: {
            $add: [ "$_id.t", { $toInt: "$r.k" } ]
          },
          r: "$r.v"
        }
      },
      {
        $match: {
          t: {
            $gt: options.startTime,
            $lt: options.endTime
          }
        }
      },
      {
        $bucketAuto: {
          groupBy: "$t",
          buckets: options.buckets,
          output: output
        }
      }
    ];

    return await new Promise((resolve, reject) => {
      let count = 0;

      coll.aggregate(stages, { allowDiskUse: this.databaseOpts.allowDiskUse || false }).forEach((doc) => {
        if (doc)
          ++count;
      }, (err) => {
        if (err)
          return reject();

        resolve(count);
      });
    });
  }

  async queryTimeComplexDifference(name, options, interval) {
    options.select = options.select || {};

    let coll = this.db.collection(`${options.group}_time_complex`);

    let oStartTime = options.startTime.getTime();
    let oEndTime = options.endTime.getTime();

    let bucketTime = chooseBucketTime(interval);
    let startTime = oStartTime - (oStartTime % bucketTime);
    let endTime = oEndTime - (oEndTime % bucketTime);
    if (endTime < oEndTime)
      endTime += bucketTime;

    let _group = {
      _id: "$_id.d",
      _first: {$first: "$r"},
      _last: {$last: "$r"}
    };

    let stages = [
      {
        $match: {
          "_id.d": Binary(uuidParse.parse(options.device, Buffer.allocUnsafe(16)), Binary.SUBTYPE_UUID),
          "_id.t": {
            $gt: new Date(startTime),
            $lt: new Date(endTime)
          }
        }
      },
      {
        $project: {
          _id: 1,
          r: { $objectToArray: "$r" }
        }
      },
      {
        $unwind: "$r"
      },
      {
        $project: {
          t: {
            $add: [ "$_id.time", { $toInt: "$r.k" } ]
          },
          r: "$r.v"
        }
      },
      {
        $match: {
          t: {
            $gt: oStartTime,
            $lt: oEndTime
          }
        }
      },
      {
        $group: _group
      }
    ];

    let sub = {};
    await coll.aggregate(stages, { allowDiskUse: this.databaseOpts.allowDiskUse || false }).forEach((result) => {
      let o1 = {};
      let o2 = {};
      
      for (let k in result) {
        if (k === "_first")
          o1.r = result[k];
        else if (k === "_last")
          o2.r = result[k];
        else
          sub[k] = result[k];
      }

      return _.assign(sub, subtractDocs(o1, o2));
    });

    return 1;
  }

  async queryTimeComplexLastBefore(name, options, interval) {
    let coll = this.db.collection(`${options.group}_time_complex`);

    let oTime = options.time.getTime();

    let bucketTime = chooseBucketTime(interval);
    let time = oTime - (oTime % bucketTime);
    if (time < oTime)
      time += bucketTime;

    let stages = [
      {
        $match: {
          "_id.d": Binary(uuidParse.parse(options.device, Buffer.allocUnsafe(16)), Binary.SUBTYPE_UUID),
          "_id.t": {
            $lt: new Date(time)
          }
        }
      },
      {
        $sort: {
          "_id.t": -1
        }
      },
      {
        $limit: 1
      },
      {
        $project: {
          _id: 1,
          r: { $objectToArray: "$r" }
        }
      },
      {
        $unwind: "$r"
      },
      {
        $project: {
          t: {
            $add: [ "$_id.t", { $toInt: "$r.k" } ]
          },
          r: "$r.v"
        }
      },
      {
        $match: {
          t: {
            $lt: options.time
          }
        }
      },
      {
        $sort: {
          t: -1
        }
      },
      {
        $limit: 1
      }
    ];

    return await new Promise((resolve, reject) => {
      let count = 0;

      coll.aggregate(stages, { allowDiskUse: this.databaseOpts.allowDiskUse || false }).forEach((doc) => {
        if (doc)
          count++;
      }, (err) => {
        if (err)
          return reject();

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

  async recordTimeComplex(id, groupName, sample, interval) {
    let collection = this.timeComplexCollections[groupName];
    if (!collection)
      collection = await this.trainTimeComplex(groupName);

    let bucketTime = chooseBucketTime(interval);
    let bucket = sample.time - (sample.time % bucketTime);
    let offsetTime = sample.time - bucket;

    let criteria = {
      _id: {
        d: Binary(uuidParse.parse(sample.device, Buffer.allocUnsafe(16)), Binary.SUBTYPE_UUID),
        t: new Date(bucket)
      }
    };

    let update = {
      $setOnInsert: {
        dt: Binary(uuidParse.parse(sample.deviceType, Buffer.allocUnsafe(16)), Binary.SUBTYPE_UUID),
        bt: bucketTime
      },
      $set: {}
    };

    update.$set[`r.${offsetTime}`] = sample[groupName].value;

    let options = {
      upsert: true
    };

    await collection.updateOne(criteria, update, options);
  }

  async recordInterval(id, groupName, sample) {
    let collection = this.intervalCollections[groupName];
    if (!collection)
      collection = await this.trainIntervalComplex(groupName);

    let criteria = {
      _id: Binary(uuidParse.parse(sample.id, Buffer.allocUnsafe(16)), Binary.SUBTYPE_UUID),
      d: Binary(uuidParse.parse(sample.device, Buffer.allocUnsafe(16)), Binary.SUBTYPE_UUID)
    };

    let update = {
      $setOnInsert: {
        dt: Binary(uuidParse.parse(sample.deviceType, Buffer.allocUnsafe(16)), Binary.SUBTYPE_UUID),
        st: sample.startTime,
        et: sample.endTime
      },
      $set: {
        v: sample.value
      }
    };

    let options = {
      upsert: true
    };

    await collection.updateOne(criteria, update, options);
  }

};
