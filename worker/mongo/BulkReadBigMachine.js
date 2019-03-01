"use strict";

const BaseBulkReadBigMachine = require("../base/BulkReadBigMachine");
const MongoMachineSink = require("./MongoMachineSink");

module.exports = class MongoBulkReadBigMachine extends BaseBulkReadBigMachine {

  constructor(id, workerId, workloadOpts, databaseOpts, mqttClient) {
    super(id, workerId, workloadOpts, mqttClient);

    this.sink = new MongoMachineSink(databaseOpts);
  }

};