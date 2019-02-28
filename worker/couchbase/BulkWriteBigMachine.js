"use strict";

const BaseBulkWriteBigMachine = require(`../base/BulkWriteBigMachine`);
const CouchbaseMachineSink = require(`./CouchbaseMachineSink`);

module.exports = class MongoBulkWriteBigMachine extends BaseBulkWriteBigMachine {

  constructor(id, workerId, workloadOpts, databaseOpts, mqttClient) {
    super(id, workerId, workloadOpts, mqttClient);

    this.sink = new CouchbaseMachineSink(databaseOpts);
  }

};