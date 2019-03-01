"use strict";

const BaseRealTimeBigMachine = require("../base/RealTimeBigMachine");
const CouchbaseMachineSink = require("./CouchbaseMachineSink");

module.exports = class MongoRealTimeBigMachine extends BaseRealTimeBigMachine {

  constructor(id, workerId, workloadOpts, databaseOpts, mqttClient) {
    super(id, workerId, workloadOpts, mqttClient);

    this.sink = new CouchbaseMachineSink(databaseOpts);
  }

};