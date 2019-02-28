"use strict";

const BaseRealTimeMidMachine = require(`../base/RealTimeMidMachine`);
const CouchbaseMachineSink = require(`./CouchbaseMachineSink`);

module.exports = class MongoRealTimeBigMachine extends BaseRealTimeMidMachine {

  constructor(id, workerId, workloadOpts, databaseOpts, mqttClient) {
    super(id, workerId, workloadOpts, mqttClient);

    this.sink = new CouchbaseMachineSink(databaseOpts);
  }

};