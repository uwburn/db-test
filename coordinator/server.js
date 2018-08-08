const uuidv4 = require(`uuid/v4`);
const mqtt = require(`mqtt`);
const Qlobber = require(`qlobber`).Qlobber;

const CHECK_WORKERS_INTERVAL = 1000;
const workers = {};

const matcher = new Qlobber({
    separator: `/`,
    wildcard_one: `+`,
    wildcard_some: `#`
});
matcher.add(`worker/+/status`, workerStatus);
matcher.add(`worker/+/work/+/status`, workStatus);
matcher.add(`worker/+/work/+/log`, workLog);

const suite = require(`./suites/${process.env.SUITE}`);
let status = `WAITING_WORKERS`;
let stepIndex = 0;

const mqttClient  = mqtt.connect(process.env.MQTT_ADDRESS);

mqttClient.on(`connect`, function () {
    console.log(`Coordinator connected to MQTT`);
    mqttClient.subscribe(`worker/#`);
});

mqttClient.on(`message`, function (topic, message) {
    let handlers = matcher.match(topic);
    if (!handlers.length)
        return;

    let levels = topic.split(`/`);
    message = JSON.parse(message);

    handlers[0](levels, message);
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

    console.log(`Worker: ${workerId}, work: ${workId}, records: ${message.samplesRecorded}/${message.totalSamples}, errors: ${message.errors}, ${Math.round(message.samplesRecorded/message.totalSamples*100)}%`);
}

function waitForWorkers() {
    if (status !== `WAITING_WORKERS`)
        return;

    let readyWorkersCount = 0;
    let busyWorkersCount = 0;
    for (let workerId in workers) {
        let worker = workers[workerId];
        switch(worker.status) {
            case `READY`:
                ++readyWorkersCount;
                break;
            case `BUSY`:
                ++busyWorkersCount;
                break;
        }
    }

    let ready = true;
    if (readyWorkersCount < suite.workers) {
        console.log(`${readyWorkersCount} ready workers, ${suite.workers} needed`);
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

    suite.steps[stepIndex].workers[workerId] = message;

    for (workerId in suite.steps[stepIndex].workers) {
        let worker = suite.steps[stepIndex].workers[workerId];
        if (worker.status !== "COMPLETED")
            return;
    }

    nextStep();
}

function nextStep() {
    ++stepIndex;
    if (stepIndex >= suite.steps.length) {
        console.log(`Suite completed`);
        shutdown();
        return;
    }

    status = `WAITING_WORKERS`;
}

function startStep() {
    status = `RUNNING_STEP`;

    let step = suite.steps[stepIndex];

    console.log(`Starting step ${step.name}`);

    let workId = uuidv4();
    let workForWorker = {
        database: suite.database,
        databaseOpts: suite.databaseOpts,
        workload: step.workload,
        workloadOpts: step.workloadOpts
    };

    step.workers = {};

    for (let workerId in workers) {
        step.workers[workerId] = {
            status: "PENDING"
        }
    }

    mqttClient.publish(`work/${workId}`, JSON.stringify(workForWorker));
}

function shutdown() {
    console.log(`Shutting down workers`);

    mqttClient.publish(`shutdown`, JSON.stringify({}), function() {
        mqttClient.end(false, function() {
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