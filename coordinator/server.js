"use strict";

const uuidv4 = require(`uuid/v4`);
const mqtt = require(`mqtt`);
const Qlobber = require(`qlobber`).Qlobber;
const envConfig = require(`env-config`);

const DECIMAL_DIGITS = 2;

const DISCOVER_WORKERS_TIMEOUT = 3000;
const CHECK_WORKERS_INTERVAL = 1000;
const workers = {};
let workersCount = 0;

const matcher = new Qlobber({
  separator: `/`,
  wildcard_one: `+`,
  wildcard_some: `#`
});
matcher.add(`worker/+/status`, workerStatus);
matcher.add(`worker/+/work/+/status`, workStatus);
matcher.add(`worker/+/work/+/log`, workLog);

const config = envConfig({});
const suite = require(`./buildSuite`)(config.database, config.databaseOpts, config.suite, config.suiteOpts);
let status = `DISCOVERING_WORKERS`;
let stepIndex = 0;
let steps = [];

let mqttClient;
suite.prepareDatabase().then(function () {
  mqttClient = mqtt.connect(process.env.MQTT_ADDRESS);

  mqttClient.on(`connect`, function () {
    console.log(`Coordinator connected to MQTT`);
    mqttClient.subscribe(`worker/#`);

    console.log(`Discovering workers`);
    setTimeout(() => {
      status = `WAITING_WORKERS`;
      workersCount = Object.keys(workers).length;
    }, DISCOVER_WORKERS_TIMEOUT);
  });

  mqttClient.on(`message`, function (topic, message) {
    let handlers = matcher.match(topic);
    if (!handlers.length)
      return;

    let levels = topic.split(`/`);
    message = JSON.parse(message);

    handlers[0](levels, message);
  });
});

function workerStatus(levels, message) {
  let workerId = levels[1];
  message.time = new Date().getTime();
  workers[workerId] = message;

  waitForWorkers();
}

function workStatus(levels, message) {
  let workerId = levels[1];
  let workId = levels[3];

  checkStep(workerId, workId, message);
}

function workLog(levels, message) {
  let workerId = levels[1];
  let workId = levels[3];

  let d = Math.pow(10, DECIMAL_DIGITS);

  let readLatency = Math.round(message.readLatency * d) / d;
  let writeLatency = Math.round(message.writeLatency * d) / d;

  console.log(`Worker: ${workerId}, R/W: ${message.reads}/${message.writes}, avg. latency R/W: ${readLatency}/${writeLatency}, errors: ${message.errors}, ${message.percent}%`);
}

function waitForWorkers() {
  if (status !== `WAITING_WORKERS`)
    return;

  let readyWorkersCount = 0;
  let busyWorkersCount = 0;
  for (let workerId in workers) {
    let worker = workers[workerId];
    switch (worker.status) {
      case `READY`:
        ++readyWorkersCount;
        break;
      case `BUSY`:
        ++busyWorkersCount;
        break;
    }
  }

  let ready = true;
  if (readyWorkersCount < workersCount) {
    console.log(`${readyWorkersCount} ready workers, ${workersCount} needed`);
    ready = false;
  }
  if (busyWorkersCount > 0) {
    console.log(`${busyWorkersCount} workers are busy`);
    ready = false;
  }

  if (!ready)
    return;

  startStep();
}

function checkStep(workerId, workId, message) {
  if (message.status === "REJECTED") {
    console.log(`Worker ${workerId} rejected work ${workId}, aborting`);
    shutdown();
  }

  steps[stepIndex].workers[workerId] = message;

  for (workerId in steps[stepIndex].workers) {
    let worker = steps[stepIndex].workers[workerId];
    if (worker.status !== "COMPLETED")
      return;
  }

  steps[stepIndex].endTime = new Date().getTime();

  logStep();

  nextStep();
}

function logStep() {
  console.log("Step completed");

  let stats = {
    totalTime: 0,
    totalReads: 0,
    avgReadLatency: 0,
    totalWrites: 0,
    avgWriteLatency: 0,
    totalErrors: 0,
    workers: 0
  }

  for (let workerId in steps[stepIndex].workers) {
    ++stats.workers;
    let worker = steps[stepIndex].workers[workerId];
    stats.totalTime += (worker.stats.endTime - worker.stats.startTime);
    stats.totalReads += worker.stats.reads;
    stats.avgReadLatency += worker.stats.readLatency;
    stats.totalWrites += worker.stats.writes;
    stats.avgWriteLatency += worker.stats.writeLatency;
    stats.totalErrors += worker.stats.errors;
  }

  stats.wps = stats.totalWrites * 1000 / stats.totalTime * stats.workers;
  stats.rps = stats.totalReads * 1000 / stats.totalTime * stats.workers;
  stats.avgReadLatency /= stats.workers;
  stats.avgWriteLatency /= stats.workers;

  steps[stepIndex].stats = stats;

  let d = Math.pow(10, DECIMAL_DIGITS);

  let readLatency = Math.round(stats.avgReadLatency * d) / d;
  let writeLatency = Math.round(stats.avgWriteLatency * d) / d;

  console.log(`Total time: ${Math.round(stats.totalTime / 1000)} s, OPS R/W: ${Math.round(stats.wps)}/${Math.round(stats.rps)}, avg. latency R/W: ${readLatency}/${writeLatency}  errors: ${stats.totalErrors}`);
}

function logSuite() {
  console.log(`Suite completed`);

  let stats = {
    totalTime: 0,
    totalWrites: 0,
    totalReads: 0,
    totalErrors: 0,
    wps: 0,
    rps: 0
  };

  for (let step of steps) {
    stats.totalTime += step.stats.totalTime;
    stats.totalWrites += step.stats.totalWrites;
    stats.totalReads += step.stats.totalReads;
    stats.totalErrors += step.stats.totalErrors;
    stats.wps += step.stats.wps * step.stats.totalTime;
    stats.rps += step.stats.rps * step.stats.totalTime;
  }

  stats.wps = stats.wps / stats.totalTime;
  stats.rps = stats.rps / stats.totalTime;

  console.log(`Total time: ${Math.round(stats.totalTime / 1000)} s, writes per second: ${Math.round(stats.wps)}, reads per second: ${Math.round(stats.rps)}, errors: ${stats.totalErrors}`);
}

function nextStep() {
  ++stepIndex;
  if (stepIndex >= suite.length) {
    logSuite();
    shutdown();
    return;
  }

  status = `WAITING_WORKERS`;
}

function startStep() {
  status = `RUNNING_STEP`;

  steps[stepIndex] = {
    startTime: new Date().getTime(),
    workers: {}
  };

  console.log(`Starting step ${suite.getStepName(stepIndex)}`);

  let workId = uuidv4();

  let workerIndex = 0;
  for (let workerId in workers) {
    let stepForWorker = suite.getStepForWorker(stepIndex, workerIndex, workersCount);

    steps[stepIndex].workers[workerId] = {
      status: "PENDING"
    };

    mqttClient.publish(`worker/${workerId}/work/${workId}`, JSON.stringify(stepForWorker));

    ++workerIndex;
  }
}

function shutdown() {
  console.log(`Shutting down workers`);

  mqttClient.publish(`shutdown`, JSON.stringify({}), function () {
    mqttClient.end(false, function () {
      process.exit(0);
    });
  });
}

setInterval(() => {
  let now = new Date().getTime();

  for (let workerId in workers) {
    let workerStatus = workers[workerId];
    if (now - workerStatus.time >= 60000) {
      console.log(`Worker ${workerId} timed out, aborting`);
      shutdown();
    }
  }
}, CHECK_WORKERS_INTERVAL);