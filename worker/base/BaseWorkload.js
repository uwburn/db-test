"use strict";

const LOG_INTERVAL = 10000;

module.exports = class BaseWorkload {

  constructor(id, workerId, mqttClient) {
    this.id = id;
    this.workerId = workerId;
    this.mqttClient = mqttClient;
  }

  getDbInterface() {
    throw new Error("Base class doesn't implement getDbInterface method");
  }

  log() {
    let stats = this.getStats();

    //this.localLog(stats);

    this.mqttClient.publish(`worker/${this.workerId}/work/${this.id}/log`, JSON.stringify(stats));
  }

  getStats() {
    throw new Error("Base class doesn't implement getStats method");
  }

  localLog(stats) {
    throw new Error("Base class doesn't implement localLog method");
  }

  async run() {
    await this.init();

    let logInterval = setInterval(this.log.bind(this), LOG_INTERVAL);

    await this._run();

    clearInterval(logInterval);

    await this.cleanup();
  }

  async init() {
    await this.getDbInterface().init();
  }

  async _run() {
    throw new Error("Base class doesn't implement _run method");
  }

  async cleanup() {
    await this.getDbInterface().cleanup();
  }

};