"use strict";

const BaseBulkWriteSmallMachine = require("../base/BulkWriteSmallMachine");
const CouchbaseMachineSink = require("./CouchbaseMachineSink");

module.exports = class MongoBulkWriteSmallMachine extends BaseBulkWriteSmallMachine {

  constructor(id, workerId, workloadOpts, databaseOpts, mqttClient) {
    super(id, workerId, workloadOpts, mqttClient);

    this.sink = new CouchbaseMachineSink(databaseOpts);
  }

};