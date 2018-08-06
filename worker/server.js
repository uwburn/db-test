const uuidv4 = require(`uuid/v4`);
const mqtt = require(`mqtt`);
const Qlobber = require(`qlobber`).Qlobber;

const workerId = uuidv4();

const mqttClient  = mqtt.connect(process.env.MQTT_ADDRESS);

const WORKER_STATUS_INTERVAL = 5000;

const matcher = new Qlobber({
    separator: `/`,
    wildcard_one: `+`,
    wildcard_some: `#`
});
matcher.add(`work/+`, addWork);
matcher.add(`shutdown`, shutdown);

let status = `READY`;
let currentWorkload;
 
mqttClient.on(`connect`, function () {
    console.log(`Worker ${workerId} connected to MQTT`);
    mqttClient.subscribe(`work/+`);
    mqttClient.subscribe(`shutdown`);

    publishWorkerStatusInterval = setInterval(function() {
        mqttClient.publish(`worker/${workerId}/status`, JSON.stringify({
            status: status, 
            work: currentWorkload ? currentWorkload.id : undefined
        }));
    }, WORKER_STATUS_INTERVAL);
});

mqttClient.on(`message`, function (topic, message) {
    let handlers = matcher.match(topic);
    if (!handlers.length)
        return;

    let levels = topic.split(`/`);
    message = JSON.parse(message);

    handlers[0](levels, message);
});

async function addWork(levels, message) {
    let workId = levels[1];

    const Workload = require(`./${message.database}/${message.workload}`);

    if (currentWorkload) {
        mqttClient.publish(`worker/${workerId}/work/${workId}/status`, JSON.stringify({
            status: "REJECTED"
        }));
        return;
    }

    console.log(`Accepting work ${workId}`)
    currentWorkload = new Workload(workId, workerId, message.workloadOpts, message.databaseOpts, mqttClient);
    status = `BUSY`;
    mqttClient.publish(`worker/${workerId}/work/${workId}/status`, JSON.stringify({
        status: "IN_PROGRESS"
    }));

    await currentWorkload.run();
    console.log(`Work ${workId} completed`)
    currentWorkload = undefined;
    status = `READY`;
    mqttClient.publish(`worker/${workerId}/work/${workId}/status`, JSON.stringify({
        status: "COMPLETED"
    }));
}

function shutdown() {
    mqttClient.end(false, function() {
        process.exit(0);
    });   
}