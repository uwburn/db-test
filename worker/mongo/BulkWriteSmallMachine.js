"use strict";

const BaseBulkWriteSmallMachine = require(`../base/BulkWriteSmallMachine`);
const MongoMachineSink = require(`./MongoMachineSink`);

module.exports = class MongoBulkWriteSmallMachine extends BaseBulkWriteSmallMachine {

  constructor(id, workerId, workloadOpts, databaseOpts, mqttClient) {
    super(id, workerId, workloadOpts, mqttClient);

    this.sink = new MongoMachineSink(databaseOpts);
  }

};