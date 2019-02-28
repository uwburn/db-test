"use strict";

const MongoClient = require('mongodb').MongoClient;
const Binary = require('mongodb').Binary;
const uuidParse = require('uuid-parse');
const _ = require('lodash');
const FlattenJS = require('flattenjs');

const BaseSink = require(`../base/BaseSink`);

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
  if (interval <= 5000)
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
    super(64, 64);

    this.databaseOpts = databaseOpts;

    this.timeComplexCollections = { };
    this.intervalCollections = { };
  }

  async init() {
    this.mongoClient = await MongoClient.connect(this.databaseOpts.url, this.databaseOpts.options);
    this.db = this.mongoClient.db(`db-test`);
    this.timeComplexColl = this.db.collection(`timeComplex`);
    this.intervalColl = this.db.collection(`interval`);

    super.init();
  }

  async cleanup() {
    super.cleanup();

    this.mongoClient.close();
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
    options.select = options.select || {};

    if (options.groups.length === 1)
      return await this.queryIntervalRangeSingleGroup(name, options);
    else
      return await this.queryIntervalRangeMultipleGroups(name, options);
  }

  async queryIntervalRangeSingleGroup(name, options) {
    let group = options.groups[0];
    let coll = this.db.collection(`${group}_interval`);

    let criteria = {
      device: Binary(uuidParse.parse(options.device, Buffer.allocUnsafe(16)), Binary.SUBTYPE_UUID),
      startTime: { $lt: options.endTime },
      endTime: { $gt: options.startTime }
    };

    return await new Promise((resolve, reject) => {
      let count = 0;

      coll.find(criteria).forEach((doc) => {
        ++count;
      }, (err) => {
        if (err)
          return reject();

        resolve(count);
      });
    });
  }

  async queryIntervalRangeMultipleGroups(name, options) {
    throw new Error("Currently not supported, as no query requires it");
  }

  async queryTimeComplexRange(name, options, interval) {
    options.select = options.select || {};

    if (options.groups.length === 1)
      return await this.queryTimeComplexRangeSingleGroup(name, options, interval);
    else
      return await this.queryTimeComplexRangeMultiGroups(name, options, interval);
  }

  async queryTimeComplexRangeSingleGroup(name, options, interval) {
    let group = options.groups[0];
    let coll = this.db.collection(`${group}_time_complex`);

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
          "_id.device": Binary(uuidParse.parse(options.device, Buffer.allocUnsafe(16)), Binary.SUBTYPE_UUID),
          "_id.time": {
            $gt: new Date(startTime),
            $lt: new Date(endTime)
          }
        }
      },
      {
        $project: {
          _id: 1,
          records: { $objectToArray: "$records" }
        }
      },
      {
        $unwind: "$records"
      },
      {
        $project: {
          time: {
            $add: [ "$_id.time", { $toInt: "$records.k" } ]
          },
          record: "$records.v"
        }
      },
      {
        $match: {
          time: {
            $gt: options.startTime,
            $lt: options.endTime
          }
        }
      }
    ];

    return await new Promise((resolve, reject) => {
      let count = 0;

      let cursor = coll.aggregate(stages, { allowDiskUse: true }).forEach((doc) => {
        count++
      }, (err) => {
        if (err)
          return reject();

        resolve(count);
      });
    });
  }

  async queryTimeComplexRangeMultiGroups(name, options, interval) {
    throw new Error("Currently not supported, as no query requires it");
  }

  async queryTimeComplexRangeBucketAvg(name, options, interval) {
    options.select = options.select || {};

    if (options.groups.length === 1)
      return await this.queryTimeComplexRangeBucketAvgSingleGroup(name, options, interval);
    else
      return await this.queryTimeComplexRangeBucketAvgMultiGroups(name, options, interval);
  }

  async queryTimeComplexRangeBucketAvgSingleGroup(name, options, interval) {
    let group = options.groups[0];
    let coll = this.db.collection(`${group}_time_complex`);

    let oStartTime = options.startTime.getTime();
    let oEndTime = options.endTime.getTime();

    let bucketTime = chooseBucketTime(interval);
    let startTime = oStartTime - (oStartTime % bucketTime);
    let endTime = oEndTime - (oEndTime % bucketTime);
    if (endTime < oEndTime)
      endTime += bucketTime;

    let output = {
      count: { $sum: 1 }
    };

    let select = options.select[group];
    for (let k in select) {
      let path = select[k];
      output[path] =  { $avg: `$record.${path}` };
    }

    let stages = [
      {
        $match: {
          "_id.device": Binary(uuidParse.parse(options.device, Buffer.allocUnsafe(16)), Binary.SUBTYPE_UUID),
          "_id.time": {
            $gt: new Date(startTime),
            $lt: new Date(endTime)
          }
        }
      },
      {
        $project: {
          _id: 1,
          records: { $objectToArray: "$records" }
        }
      },
      {
        $unwind: "$records"
      },
      {
        $project: {
          time: {
            $add: [ "$_id.time", { $toInt: "$records.k" } ]
          },
          record: "$records.v"
        }
      },
      {
        $match: {
          time: {
            $gt: options.startTime,
            $lt: options.endTime
          }
        }
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

      coll.aggregate(stages, { allowDiskUse: true }).forEach((doc) => {
        ++count;
      }, (err) => {
        if (err)
          return reject();

        resolve(count);
      });
    });
  }

  async queryTimeComplexRangeBucketAvgMultiGroups(name, options, interval) {
    throw new Error("Currently not supported, as no query requires it");
  }

  async queryTimeComplexDifference(name, options, interval) {
    options.select = options.select || {};

    if (options.groups.length === 1)
      return await this.queryTimeComplexDifferenceSingleGroup(name, options, interval);
    else
      return await this.queryTimeComplexDifferenceMultiGroups(name, options, interval);
  }

  async queryTimeComplexDifferenceSingleGroup(name, options, interval) {
    let group = options.groups[0];
    let coll = this.db.collection(`${group}_time_complex`);

    let bucketTime = chooseBucketTime(interval);
    let promises = [];
    for (let i = 0; i < options.times.length -1; ++i) {
      let oStartTime = options.times[i].getTime();
      let oEndTime = options.times[i + 1].getTime();

      let startTime = oStartTime - (oStartTime % bucketTime);
      let endTime = oEndTime - (oEndTime % bucketTime);
      if (endTime < oEndTime)
        endTime += bucketTime;

      let _group = {
        _id: "$_id.device"
      };
      let select = options.select[group];
      let hasPath = false;
      for (let k in select) {
        let path = select[k];
        hasPath = true;
        _group[path + "_first"] = {$first: "$record." + path};
        _group[path + "_last"] = {$last: "$record." + path};
      }
      if (!hasPath) {
        _group["_first"] = {$first: "$record"};
        _group["_last"] = {$last: "$record"};
      }

      let stages = [
        {
          $match: {
            "_id.device": Binary(uuidParse.parse(options.device, Buffer.allocUnsafe(16)), Binary.SUBTYPE_UUID),
            "_id.time": {
              $gt: new Date(startTime),
              $lt: new Date(endTime)
            }
          }
        },
        {
          $project: {
            _id: 1,
            records: { $objectToArray: "$records" }
          }
        },
        {
          $unwind: "$records"
        },
        {
          $project: {
            time: {
              $add: [ "$_id.time", { $toInt: "$records.k" } ]
            },
            record: "$records.v"
          }
        },
        {
          $match: {
            time: {
              $gt: options.times[i],
              $lt: options.times[i+1]
            }
          }
        },
        {
          $group: _group
        }
      ];

      promises.push(coll.aggregate(stages, { allowDiskUse: true }).toArray());
    }

    let results = await Promise.all(promises);

    let subs = results.map((result) => {
      result = result[0];

      let o1 = {};
      let o2 = {};
      let sub = {};
      for (let k in result) {
        if (k === "_first")
          o1.record = result[k];
        else if (k.endsWith("_first"))
          o1[k.substring(0, k.length - 6)] = result[k];
        else if (k === "_last")
          o2.record = result[k];
        else if (k.endsWith("_last"))
          o2[k.substring(0, k.length - 5)] = result[k];
        else
          sub[k] = result[k];
      }

      return _.assign(sub, subtractDocs(o1, o2));
    });

    return subs.length;
  }

  async queryTimeComplexDifferenceMultiGroups(name, options, interval) {
    throw new Error("Currently not supported, as no query requires it");
  }

  async queryTimeComplexLastBefore(name, options, interval) {
    options.select = options.select || {};

    if (options.groups.length === 1)
      return await this.queryTimeComplexLastBeforeSingleGroup(name, options, interval);
    else
      return await this.queryTimeComplexLastBeforeMultiGroups(name, options, interval);
  }

  async queryTimeComplexLastBeforeSingleGroup(name, options, interval) {
    let group = options.groups[0];
    let coll = this.db.collection(`${group}_time_complex`);

    let oTime = options.time.getTime();

    let bucketTime = chooseBucketTime(interval);
    let time = oTime - (oTime % bucketTime);
    if (time < oTime)
      time += bucketTime;

    let stages = [
      {
        $match: {
          "_id.device": Binary(uuidParse.parse(options.device, Buffer.allocUnsafe(16)), Binary.SUBTYPE_UUID),
          "_id.time": {
            $lt: new Date(time)
          }
        }
      },
      {
        $sort: {
          "_id.time": -1
        }
      },
      {
        $limit: 1
      },
      {
        $project: {
          _id: 1,
          records: { $objectToArray: "$records" }
        }
      },
      {
        $unwind: "$records"
      },
      {
        $project: {
          time: {
            $add: [ "$_id.time", { $toInt: "$records.k" } ]
          },
          record: "$records.v"
        }
      },
      {
        $match: {
          time: {
            $lt: options.time
          }
        }
      },
      {
        $sort: {
          time: -1
        }
      },
      {
        $limit: 1
      }
    ];

    return await new Promise((resolve, reject) => {
      let count = 0;

      let cursor = coll.aggregate(stages, { allowDiskUse: true }).forEach((doc) => {
        count++
      }, (err) => {
        if (err)
          return reject();

        resolve(count);
      });
    });
  }

  async queryTimeComplexLastBeforeMultiGroups(name, options, interval) {
    throw new Error("Currently not supported, as no query requires it");
  }

  async queryTimeComplexTopDifference(name, options, interval) {
    options.select = options.select || {};

    if (options.groups.length === 1)
      return await this.queryTimeComplexTopDifferenceSingleGroup(name, options, interval);
    else
      return await this.queryTimeComplexTopDifferenceMultiGroups(name, options, interval);
  }

  async queryTimeComplexTopDifferenceSingleGroup(name, options, interval) {
    let group = options.groups[0];
    let coll = this.db.collection(`${group}_time_complex`);

    let oStartTime = options.startTime.getTime();
    let oEndTime = options.endTime.getTime();

    let bucketTime = chooseBucketTime(interval);
    let startTime = oStartTime - (oStartTime % bucketTime);
    let endTime = oEndTime - (oEndTime % bucketTime);
    if (endTime < oEndTime)
      endTime += bucketTime;

    let _group = {
      _id: "$_id.device"
    };
    let select = options.select[group];
    let hasPath = false;
    for (let k in select) {
      let path = select[k];
      hasPath = true;
      _group[path + "_first"] = {$first: "$record." + path};
      _group[path + "_last"] = {$last: "$record." + path};
    }
    if (!hasPath) {
      _group["_first"] = {$first: "$record"};
      _group["_last"] = {$last: "$record"};
    }

    let stages = [
      {
        $match: {
          "_id.time": {
            $gt: new Date(startTime),
            $lt: new Date(endTime)
          }
        }
      },
      {
        $project: {
          _id: 1,
          records: { $objectToArray: "$records" }
        }
      },
      {
        $unwind: "$records"
      },
      {
        $project: {
          time: {
            $add: [ "$_id.time", { $toInt: "$records.k" } ]
          },
          record: "$records.v"
        }
      },
      {
        $match: {
          time: {
            $gt: options.startTime,
            $lt: options.endTime
          }
        }
      },
      {
        $group: _group
      }
    ];

    let subs = [];
    await new Promise((resolve, reject) => {
      coll.aggregate(stages, { allowDiskUse: true }).forEach((doc) => {
        let o1 = {};
        let o2 = {};
        let sub = {};
        for (let k in doc) {
          if (k === "_first")
            o1.record = doc[k];
          else if (k.endsWith("_first"))
            o1[k.substring(0, k.length - 6)] = doc[k];
          else if (k === "_last")
            o2.record = doc[k];
          else if (k.endsWith("_last"))
            o2[k.substring(0, k.length - 5)] = doc[k];
          else
            sub[k] = doc[k];
        }

        subs.push(_.assign(sub, subtractDocs(o1, o2)));
      }, (err) => {
        if (err)
          return reject();

        resolve();
      });
    });

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

  async queryTimeComplexTopDifferenceMultiGroups(name, options, interval) {
    throw new Error("Currently not supported, as no query requires it");
  }

  async queryIntervalTopCount(name, options) {
    options.select = options.select || {};

    if (options.groups.length === 1)
      return await this.queryIntervalTopCountSingleGroup(name, options);
    else
      return await this.queryIntervalTopCountMultiGroups(name, options);
  }

  async queryIntervalTopCountSingleGroup(name, options) {
    let group = options.groups[0];
    let coll = this.db.collection(`${group}_interval`);

    options.select = options.select || {};

    let stages = [
      {
        $match: {
          startTime: { $lt: options.endTime },
          endTime: { $gt: options.startTime }
        }
      },
      {
        $group: {
          _id: "$device",
          count: { $sum: 1 }
        }
      },
      {
        $sort: {
          count: -1
        }
      },
      {
        $limit : options.limit
      }
    ];

    return await new Promise((resolve, reject) => {
      let count = 0;

      coll.aggregate(stages, { allowDiskUse: true }).forEach((doc) => {
        ++count;
      }, (err) => {
        if (err)
          return reject();

        resolve(count);
      });
    });
  }

  async queryIntervalTopCountMultiGroups(name, options) {
    throw new Error("Currently not supported, as no query requires it");
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
    if (!collection) {
      collection = this.db.collection(`${groupName}_time_complex`);
      try {
        await collection.createIndex({ "_id.device": 1 });
        await collection.createIndex({ "_id.time": 1 });
        await collection.createIndex({ "_id.device": 1, "_id.time": 1 });
        await collection.createIndex({ "_id.device": 1, "_id.time": -1 });
      }
      catch(err) { }
      this.timeComplexCollections[groupName] = collection;
    }

    let bucketTime = chooseBucketTime(interval);
    let bucket = sample.time - (sample.time % bucketTime);
    let offsetTime = sample.time - bucket;

    let criteria = {
      _id: {
        device: Binary(uuidParse.parse(sample.device, Buffer.allocUnsafe(16)), Binary.SUBTYPE_UUID),
        time: new Date(bucket)
      }
    };

    let update = {
      $setOnInsert: {
        deviceType: Binary(uuidParse.parse(sample.deviceType, Buffer.allocUnsafe(16)), Binary.SUBTYPE_UUID),
        bucketTime: bucketTime
      },
      $set: {}
    };

    update.$set[`records.${offsetTime}`] = sample[groupName].value;

    let options = {
      upsert: true
    };

    await collection.updateOne(criteria, update, options);
  }

  async recordInterval(id, groupName, sample) {
    let collection = this.intervalCollections[groupName];
    if (!collection) {
      collection = this.db.collection(`${groupName}_interval`);
      try {
        await collection.createIndex({ device: 1 });
        await collection.createIndex({ startTime: 1 });
        await collection.createIndex({ endTime: 1 });
        await collection.createIndex({ startTime: 1, endTime: -1 });
        await collection.createIndex({ device: 1, startTime: 1, endTime: -1 });
      }
      catch(err) { }
      this.intervalCollections[groupName] = collection;
    }

    let criteria = {
      _id: Binary(uuidParse.parse(sample.id, Buffer.allocUnsafe(16)), Binary.SUBTYPE_UUID),
    };

    let update = {
      $setOnInsert: {
        deviceType: Binary(uuidParse.parse(sample.deviceType, Buffer.allocUnsafe(16)), Binary.SUBTYPE_UUID),
        device: Binary(uuidParse.parse(sample.device, Buffer.allocUnsafe(16)), Binary.SUBTYPE_UUID),
        startTime: sample.startTime,
        endTime: sample.endTime
      },
      $set: {
        value: sample.value
      }
    };

    let options = {
      upsert: true
    };

    await collection.updateOne(criteria, update, options);
  }

};