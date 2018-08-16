"use strict";

const LOG_INTERVAL = 10000;

module.exports = class BaseWorkload {

  constructor(id, workerId, mqttClient) {
    this.id = id;
    this.workerId = workerId;
    this.mqttClient = mqttClient;
  }

  log() {
    this.mqttClient.publish(`worker/${this.workerId}/work/${this.id}/log`, JSON.stringify(this.stats()));
  }

  stats() {
    throw new Error("Base class doesn't implement stats method");
  }

  async run() {
    await this.init();

    let logInterval = setInterval(this.log.bind(this), LOG_INTERVAL);

    await this._run();

    clearInterval(logInterval);

    await this.cleanup();
  }

  async init() {
    await this.dbInterface.init();
  }

  async _run() {
    throw new Error("Base class doesn't implement _run method");
  }

  async cleanup() {
    await this.dbInterface.cleanup();
  }

};