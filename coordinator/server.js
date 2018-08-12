const uuidv4 = require(`uuid/v4`);
const mqtt = require(`mqtt`);
const Qlobber = require(`qlobber`).Qlobber;
const envConfig = require(`env-config`);

const DISCOVER_WORKERS_TIMEOUT = 10000;
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
let step;

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

  console.log(`Worker: ${workerId}, work: ${workId}, writes: ${message.writes}, reads: ${message.reads}, errors: ${message.errors}, ${message.percent}%`);
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

  step.workers[workerId] = message;

  for (workerId in step.workers) {
    let worker = step.workers[workerId];
    if (worker.status !== "COMPLETED")
      return;
  }

  step.endTime = new Date().getTime();

  logStep();

  nextStep();
}

function logStep() {
  console.log("Step completed");

  let totalTime = 0;
  let totalWrites = 0;
  let totalReads = 0;
  let totalErrors = 0;
  let workers = 0;
  for (workerId in step.workers) {
    ++workers;
    let worker = step.workers[workerId];
    totalTime += (worker.endTime - worker.startTime);
    totalWrites += worker.writes;
    totalReads += worker.reads;
    totalErrors += worker.errors;
  }

  let wps = Math.round(totalWrites * 1000 / totalTime * workers);
  let rps = Math.round(totalReads * 1000 / totalTime * workers);

  console.log(`Total time: ${totalTime / 1000} s, writes per second: ${wps}, reads per second: ${rps}, errors: ${totalErrors}`);
}

function nextStep() {
  ++stepIndex;
  if (stepIndex >= suite.length) {
    console.log(`Suite completed`);
    shutdown();
    return;
  }

  status = `WAITING_WORKERS`;
}

function startStep() {
  status = `RUNNING_STEP`;

  step = {
    startTime: new Date().getTime(),
    workers: {}
  };

  console.log(`Starting step ${suite.getStepName(stepIndex)}`);

  let workId = uuidv4();

  let workerIndex = 0;
  for (let workerId in workers) {
    let stepForWorker = suite.getStepForWorker(stepIndex, workerIndex, workersCount);

    step.workers[workerId] = {
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