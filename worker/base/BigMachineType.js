"use strict";

const uuidv4 = require('uuid/v4');

module.exports = class BigMachineType {

  constructor(workloadOpts) {
    this.workloadOpts = workloadOpts;

    this.sampleTypes = {
      status: "INTERVAL",
      counters: "TIME_COMPLEX",
      setup: "TIME_COMPLEX",
      temperatureProbe1: "TIME_COMPLEX",
      temperatureProbe2: "TIME_COMPLEX",
      alarm: "INTERVAL"
    };

    this.nominalIntervals = {
      status: 3600000,
      counters: 120000,
      setup: 300000,
      temperatureProbe1: 1000,
      temperatureProbe2: 1000,
      alarm: 1800000
    };

    this.sampleMethods = {
      status: this.statusSample.bind(this),
      counters: this.countersSample.bind(this),
      setup: this.setupSample.bind(this),
      temperatureProbe1: this.temperatureProbe1Sample.bind(this),
      temperatureProbe2: this.temperatureProbe2Sample.bind(this),
      alarm: this.alarmSample.bind(this)
    };
  }

  sample(id, groupName, absDate) {
    return {
      type: this.sampleTypes[groupName],
      value: this.sampleMethods[groupName](id, groupName, absDate)
    }
  }

  statusSample(id, groupName, absDate) {
    return {
      id: uuidv4(),
      deviceType: this.workloadOpts.machineTypeId,
      device: id,
      startTime: absDate,
      endTime: new Date(absDate.getTime() + Math.round(Math.random() * 300 + 60) * 1000),
      value: Math.random() > 0.5 ? "WORKING" : "IDLE"
    };
  }

  countersSample(id, groupName, absDate) {
    return {
      deviceType: this.workloadOpts.machineTypeId,
      device: id,
      time: absDate,
      counters: {
        time: absDate,
        value: {
          activeEnergyConsumed: Math.round(Math.random() * 1000000),
          reactiveEnergyProduced: Math.round(Math.random() * 1000000),
          totalLengthProcessed: Math.random() * 1000000,
          partialLengthProcessed: Math.random() * 1000000,
          totalWorkedTime: Math.round(Math.random() * 1000000),
          partialWorkedTime: Math.round(Math.random() * 1000000),
          totalPartsProcessed: Math.round(Math.random() * 1000000),
          partialPartProcessed: Math.round(Math.random() * 1000000),
          clutches: [
            {
              cycles: Math.round(Math.random() * 10000)
            },
            {
              cycles: Math.round(Math.random() * 10000)
            }
          ],
          oilPumps: [
            {
              cycles: Math.round(Math.random() * 10000),
              workingTime: Math.round(Math.random() * 1000000),
            },
            {
              cycles: Math.round(Math.random() * 10000),
              workingTime: Math.round(Math.random() * 1000000),
            },
            {
              cycles: Math.round(Math.random() * 10000),
              workingTime: Math.round(Math.random() * 1000000),
            },
            {
              cycles: Math.round(Math.random() * 10000),
              workingTime: Math.round(Math.random() * 1000000),
            }
          ],
          belts: [
            {
              failures: Math.round(Math.random() * 100),
              workingTime: Math.round(Math.random() * 1000000),
            },
            {
              failures: Math.round(Math.random() * 100),
              workingTime: Math.round(Math.random() * 1000000),
            },
            {
              failures: Math.round(Math.random() * 100),
              workingTime: Math.round(Math.random() * 1000000),
            },
            {
              failures: Math.round(Math.random() * 100),
              workingTime: Math.round(Math.random() * 1000000),
            },
            {
              failures: Math.round(Math.random() * 100),
              workingTime: Math.round(Math.random() * 1000000),
            },
            {
              failures: Math.round(Math.random() * 100),
              workingTime: Math.round(Math.random() * 1000000),
            }
          ],
          lamps: [
            {
              cycles: Math.round(Math.random() * 10000),
              failures: Math.round(Math.random() * 100),
              workingTime: Math.round(Math.random() * 1000000),
            },
            {
              cycles: Math.round(Math.random() * 10000),
              failures: Math.round(Math.random() * 100),
              workingTime: Math.round(Math.random() * 1000000),
            }
          ],
          drives: [
            {
              cycles: Math.round(Math.random() * 10000),
            },
            {
              cycles: Math.round(Math.random() * 10000),
            },
            {
              cycles: Math.round(Math.random() * 10000),
            },
            {
              cycles: Math.round(Math.random() * 10000),
            },
            {
              cycles: Math.round(Math.random() * 10000),
            },
            {
              cycles: Math.round(Math.random() * 10000),
            },
            {
              cycles: Math.round(Math.random() * 10000),
            },
            {
              cycles: Math.round(Math.random() * 10000),
            }
          ]
        }
      }
    };
  }

  setupSample(id, groupName, absDate) {
    return {
      deviceType: this.workloadOpts.machineTypeId,
      device: id,
      time: absDate,
      setup: {
        time: absDate,
        value: {
          partDiameter: Math.round(Math.random() * 500),
          partThickness: Math.round(Math.random() * 10),
          partLength: Math.round(Math.random() * 5000),
          partType: Math.round(Math.random() * 5),
          partCode: Math.round(Math.random() * 10000).toString(),
          batchCode: Math.round(Math.random() * 10000).toString(),
          heaters: [
            {
              preheatPosition: Math.round(Math.random() * 1000),
              heatPosition: Math.round(Math.random() * 1000),
              temperature: Math.round(Math.random() * 200),
              preheatTime: Math.round(Math.random() * 10),
              heatTime: Math.round(Math.random() * 20)
            },
            {
              preheatPosition: Math.round(Math.random() * 1000),
              heatPosition: Math.round(Math.random() * 1000),
              temperature: Math.round(Math.random() * 200),
              preheatTime: Math.round(Math.random() * 10),
              heatTime: Math.round(Math.random() * 20)
            }
          ],
          drives: [
            {
              time: Math.round(Math.random() * 10),
              delay: Math.round(Math.random() * 5)
            },
            {
              time: Math.round(Math.random() * 10),
              delay: Math.round(Math.random() * 5)
            },
            {
              time: Math.round(Math.random() * 10),
              delay: Math.round(Math.random() * 5)
            },
            {
              time: Math.round(Math.random() * 10),
              delay: Math.round(Math.random() * 5)
            },
            {
              time: Math.round(Math.random() * 10),
              delay: Math.round(Math.random() * 5)
            },
            {
              time: Math.round(Math.random() * 10),
              delay: Math.round(Math.random() * 5)
            },
            {
              time: Math.round(Math.random() * 10),
              delay: Math.round(Math.random() * 5)
            },
            {
              time: Math.round(Math.random() * 10),
              delay: Math.round(Math.random() * 5)
            }
          ]
        }
      }
    };
  }

  temperatureProbe1Sample(id, groupName, absDate) {
    return {
      deviceType: this.workloadOpts.machineTypeId,
      device: id,
      time: absDate,
      temperatureProbe1: {
        time: absDate,
        value: Math.random() * 100 + 50
      }
    };
  }

  temperatureProbe2Sample(id, groupName, absDate) {
    return {
      deviceType: this.workloadOpts.machineTypeId,
      device: id,
      time: absDate,
      temperatureProbe2: {
        time: absDate,
        value: Math.random() * 100 + 50
      }
    };
  }

  alarmSample(id, groupName, absDate) {
    return {
      id: uuidv4(),
      deviceType: this.workloadOpts.machineTypeId,
      device: id,
      startTime: absDate,
      endTime: new Date(absDate.getTime() + Math.round(Math.random() * 300 + 60) * 1000),
      value: Math.ceil(Math.random() * 100).toString()
    };
  }

};
