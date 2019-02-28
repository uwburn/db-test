"use strict";

const BaseRealTimeMidMachine = require(`../base/RealTimeMidMachine`);
const CassandraMachineSink = require(`./CassandraMachineSink`);

module.exports = class RealTimeBigMachine extends BaseRealTimeMidMachine {

  constructor(id, workerId, workloadOpts, databaseOpts, mqttClient) {
    super(id, workerId, workloadOpts, mqttClient);

    this.sink = new CassandraMachineSink(databaseOpts);
  }

};