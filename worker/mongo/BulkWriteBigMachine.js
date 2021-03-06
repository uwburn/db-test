"use strict";

const BaseBulkWriteBigMachine = require("../base/BulkWriteBigMachine");
const MongoMachineSink = require("./MongoMachineSink");

module.exports = class MongoBulkWriteBigMachine extends BaseBulkWriteBigMachine {

  constructor(id, workerId, workloadOpts, databaseOpts, mqttClient) {
    super(id, workerId, workloadOpts, mqttClient);

    this.sink = new MongoMachineSink(databaseOpts);
  }

};