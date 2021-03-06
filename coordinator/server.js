"use strict";

const uuidv4 = require("uuid/v4");
const mqtt = require("mqtt");
const Qlobber = require("qlobber").Qlobber;
const fs = require("fs");
const _ = require("lodash");

const DECIMAL_DIGITS = 2;

const DISCOVER_WORKERS_TIMEOUT = 15000;
const CHECK_WORKERS_INTERVAL = 1000;
const WORKER_TIMEOUT = process.env.WOKRER_TIMEOUT || 60000;
const workers = {};
let workersCount = 0;

const matcher = new Qlobber({
  separator: "/",
  wildcard_one: "+",
  wildcard_some: "#"
});
matcher.add("worker/+/status", workerStatus);
matcher.add("worker/+/work/+/status", workStatus);
matcher.add("worker/+/work/+/log", workLog);

const config = require(process.env.CONFIG || "/run/secrets/coordinator_config.json");

const suite = require("./buildSuite")(config);
let status = "DISCOVERING_WORKERS";
let stepIndex = 0;
let stats = {
  description: suite.description,
  comment: suite.comment,
  database: suite.database,
  machines: suite.machines.length,
  startTime: new Date().getTime(),
  endTime: null,
  duration: 0,
  steps: []
};

let mqttClient;
suite.prepareDatabase().then(function () {
  mqttClient = mqtt.connect(process.env.MQTT_ADDRESS);

  mqttClient.on("connect", function () {
    console.log("Coordinator connected to MQTT");
    mqttClient.subscribe("worker/#");

    if (status !== "DISCOVERING_WORKERS")
      return;

    console.log("Discovering workers");
    setTimeout(() => {
      status = "WAITING_WORKERS";
      workersCount = Object.keys(workers).length;
    }, DISCOVER_WORKERS_TIMEOUT);
  });

  mqttClient.on("message", function (topic, message) {
    let handlers = matcher.match(topic);
    if (!handlers.length)
      return;

    let levels = topic.split("/");
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

  let d = Math.pow(10, DECIMAL_DIGITS);

  let readLatency = Math.round(message.readLatency * d) / d;
  let writeLatency = Math.round(message.writeLatency * d) / d;

  console.log(`Worker: ${workerId}, R/W: ${message.reads}/${message.writes}, avg. latency R/W: ${readLatency}/${writeLatency}, errors: ${message.errors}, ${message.percent}%`);
}

function waitForWorkers() {
  if (status !== "WAITING_WORKERS")
    return;

  let readyWorkersCount = 0;
  let busyWorkersCount = 0;
  for (let workerId in workers) {
    let worker = workers[workerId];
    switch (worker.status) {
    case "READY":
      ++readyWorkersCount;
      break;
    case "BUSY":
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

  stats.steps[stepIndex].workers[workerId] = message;

  for (workerId in stats.steps[stepIndex].workers) {
    let worker = stats.steps[stepIndex].workers[workerId];
    if (worker.status !== "COMPLETED")
      return;
  }

  stats.steps[stepIndex].endTime = new Date().getTime();

  logStep();

  nextStep();
}

function logStep() {
  console.log("Step completed");

  let stepStats = {
    workTime: 0,
    totalReads: 0,
    totalReadRows: 0,
    avgReadLatency: 0,
    totalWrites: 0,
    avgWriteLatency: 0,
    totalErrors: 0,
    workers: 0,
    tags: stats.steps[stepIndex].tags
  };

  for (let workerId in stats.steps[stepIndex].workers) {
    ++stepStats.workers;
    let worker = stats.steps[stepIndex].workers[workerId];
    stepStats.workTime += (worker.stats.endTime - worker.stats.startTime);
    stepStats.totalReads += worker.stats.reads;
    stepStats.totalReadRows += worker.stats.totalReadRows;
    stepStats.avgReadLatency += worker.stats.readLatency;
    stepStats.totalWrites += worker.stats.writes;
    stepStats.avgWriteLatency += worker.stats.writeLatency;
    stepStats.totalErrors += worker.stats.errors;
  }

  stepStats.wps = stepStats.totalWrites * 1000 / stepStats.workTime * stepStats.workers;
  stepStats.rps = stepStats.totalReads * 1000 / stepStats.workTime * stepStats.workers;
  stepStats.avgReadLatency /= stepStats.workers;
  stepStats.avgWriteLatency /= stepStats.workers;

  stats.steps[stepIndex] = stepStats;

  let d = Math.pow(10, DECIMAL_DIGITS);

  let readLatency = Math.round(stepStats.avgReadLatency * d) / d;
  let writeLatency = Math.round(stepStats.avgWriteLatency * d) / d;

  console.log(`Total time: ${Math.round(stepStats.workTime / 1000)} s, OPS R/W: ${Math.round(stepStats.rps)}/${Math.round(stepStats.wps)}, avg. latency R/W: ${readLatency}/${writeLatency}  errors: ${stepStats.totalErrors}`);
}

function logSuite() {
  console.log("Suite completed");

  _.assign(stats, {
    endTime: new Date().getTime(),
    workTime: 0,
    totalWrites: 0,
    totalReads: 0,
    totalReadRows: 0,
    totalErrors: 0,
    wps: 0,
    rps: 0,
    avgWriteLatency: 0,
    avgReadLatency: 0
  });
  stats.duration = stats.endTime - stats.startTime;

  stats.steps.forEach((s) => {
    stats.workTime += s.workTime;
    stats.totalWrites += s.totalWrites;
    stats.totalReads += s.totalReads;
    stats.totalReadRows += s.totalReadRows;
    stats.totalErrors += s.totalErrors;
  });

  let wpsWorkTime = 0;
  stats.steps.filter(s => s.tags.indexOf("WRITE_SPEED") >= 0)
    .forEach((s) => {
      wpsWorkTime += s.workTime;
      stats.wps += s.wps * s.workTime;
    });
  
  let rpsWorkTime = 0;
  stats.steps.filter(s => s.tags.indexOf("READ_SPEED") >= 0)
    .forEach((s) => {
      rpsWorkTime += s.workTime;
      stats.rps += s.rps * s.workTime;
    });

  let writeLatencyCount = 0;
  stats.steps.filter(s => s.tags.indexOf("WRITE_LATENCY") >= 0)
    .forEach((s) => {
      ++writeLatencyCount;
      stats.avgWriteLatency += s.avgWriteLatency;
    });

  let readLatencyCount = 0;
  stats.steps.filter(s => s.tags.indexOf("READ_LATENCY") >= 0)
    .forEach((s) => {
      ++readLatencyCount;
      stats.avgReadLatency += s.avgReadLatency;
    });

  stats.wps = stats.wps / wpsWorkTime;
  stats.rps = stats.rps / rpsWorkTime;
  stats.avgWriteLatency /= writeLatencyCount;
  stats.avgReadLatency /= readLatencyCount;

  console.log(`Total time: ${Math.round(stats.workTime / 1000)} s, writes per second: ${Math.round(stats.wps)}, reads per second: ${Math.round(stats.rps)}, errors: ${stats.totalErrors}`);
}

function nextStep() {
  ++stepIndex;
  if (stepIndex >= suite.length) {
    logSuite();
    shutdown();
    return;
  }

  status = "WAITING_WORKERS";
}

function startStep() {
  status = "RUNNING_STEP";

  stats.steps[stepIndex] = {
    startTime: new Date().getTime(),
    workers: {},
    tags: suite.getStepTags(stepIndex)
  };

  console.log(`Starting step ${suite.getStepName(stepIndex)}`);

  let workId = uuidv4();

  let workerIndex = 0;
  for (let workerId in workers) {
    let stepForWorker = suite.getStepForWorker(stepIndex, workerIndex, workersCount);

    stats.steps[stepIndex].workers[workerId] = {
      status: "PENDING"
    };

    mqttClient.publish(`worker/${workerId}/work/${workId}`, JSON.stringify(stepForWorker));

    ++workerIndex;
  }
}

function shutdown() {
  writeStats();

  console.log("Shutting down workers");

  mqttClient.publish("shutdown", JSON.stringify({}), function () {
    mqttClient.end(false, function () {
      if (process.env.NO_EXIT)
        setInterval(function() {}, 60000);
      else
        process.exit(0);
    });
  });
}

function writeStats() {
  console.log("Recording stats");

  let statsDir = process.env.STATS_DIR || "/var/log/db-test/";

  try {
    fs.mkdirSync(statsDir);
  }
  catch (err) {
    if (err.code !== "EEXIST") throw err;
  }

  fs.writeFileSync(statsDir + suite.id + ".json", JSON.stringify(stats, null, 2));
}

setInterval(() => {
  let now = new Date().getTime();

  for (let workerId in workers) {
    let workerStatus = workers[workerId];
    if (now - workerStatus.time >= WORKER_TIMEOUT) {
      console.log(`Worker ${workerId} timed out, aborting`);
      shutdown();
    }
  }
}, CHECK_WORKERS_INTERVAL);
