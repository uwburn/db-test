"use strict";

const BaseBulkWriteMidMachine = require("../base/BulkWriteMidMachine");
const CouchbaseMachineSink = require("./CouchbaseMachineSink");

module.exports = class MongoBulkWriteBigMachine extends BaseBulkWriteMidMachine {

  constructor(id, workerId, workloadOpts, databaseOpts, mqttClient) {
    super(id, workerId, workloadOpts, mqttClient);

    this.sink = new CouchbaseMachineSink(databaseOpts);
  }

};