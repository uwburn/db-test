"use strict";

const BaseRealTimeSmallMachine = require("../base/RealTimeSmallMachine");
const CouchbaseMachineSink = require("./CouchbaseMachineSink");

module.exports = class MongoRealTimeSmallMachine extends BaseRealTimeSmallMachine {

  constructor(id, workerId, workloadOpts, databaseOpts, mqttClient) {
    super(id, workerId, workloadOpts, mqttClient);

    this.sink = new CouchbaseMachineSink(databaseOpts);
  }

};