"use strict";

const BaseBulkReadSmallMachine = require(`../base/BulkReadSmallMachine`);
const CouchbaseMachineSink = require(`./CouchbaseMachineSink`);

module.exports = class MongoBulkReadSmallMachine extends BaseBulkReadSmallMachine {

  constructor(id, workerId, workloadOpts, databaseOpts, mqttClient) {
    super(id, workerId, workloadOpts, mqttClient);

    this.sink = new CouchbaseMachineSink(databaseOpts);
  }

};