"use strict";

const BaseRealTimeMidMachine = require(`../base/RealTimeMidMachine`);
const MongoMachineSink = require(`./MongoMachineSink`);

module.exports = class MongoRealTimeBigMachine extends BaseRealTimeMidMachine {

  constructor(id, workerId, workloadOpts, databaseOpts, mqttClient) {
    super(id, workerId, workloadOpts, mqttClient);

    this.sink = new MongoMachineSink(databaseOpts);
  }

};