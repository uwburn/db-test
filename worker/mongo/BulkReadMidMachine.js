"use strict";

const BaseBulkReadMidMachine = require("../base/BulkReadMidMachine");
const MongoMachineSink = require("./MongoMachineSink");

module.exports = class MongoBulkReadBigMachine extends BaseBulkReadMidMachine {

  constructor(id, workerId, workloadOpts, databaseOpts, mqttClient) {
    super(id, workerId, workloadOpts, mqttClient);

    this.sink = new MongoMachineSink(databaseOpts);
  }

};