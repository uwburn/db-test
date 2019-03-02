"use strict";

const uuidv4 = require("uuid/v4");
const moment = require("moment");

const MIN_QUERY_INTERVAL = 10000;
const MAX_QUERY_INTERVAL = 30000;

module.exports = class MidMachineSource {

  constructor(workloadOpts) {
    this.workloadOpts = workloadOpts;

    this.sampleTypes = {
      status: "TIME_COMPLEX",
      state: "INTERVAL",
      counters: "TIME_COMPLEX",
      setup: "TIME_COMPLEX",
      geo: "TIME_COMPLEX",
      alarm: "INTERVAL"
    };

    this.sampleIntervals = {
      status: 120000,
      state: 3600000,
      counters: 360000,
      setup: 360000,
      geo: 86400000,
      alarm: 1800000
    };

    this.sampleMethods = {
      status: this.statusSample.bind(this),
      state: this.stateSample.bind(this),
      counters: this.countersSample.bind(this),
      setup: this.setupSample.bind(this),
      geo: this.geoSample.bind(this),
      alarm: this.alarmSample.bind(this)
    };

    this.queryIntervals = {
      lastWeekStates: Math.min(Math.max(Math.floor(300000 / this.workloadOpts.machines.length), MIN_QUERY_INTERVAL), MAX_QUERY_INTERVAL),
      lastDayAlarms: Math.min(Math.max(Math.floor(300000 / this.workloadOpts.machines.length), MIN_QUERY_INTERVAL), MAX_QUERY_INTERVAL),
      lasWeekMachineEnergy: Math.min(Math.max(Math.floor(300000 / this.workloadOpts.machines.length), MIN_QUERY_INTERVAL), MAX_QUERY_INTERVAL),
      lastMonthCountersDifference: Math.min(Math.max(Math.floor(300000 / this.workloadOpts.machines.length), MIN_QUERY_INTERVAL), MAX_QUERY_INTERVAL),
      oldSetup: Math.min(Math.max(Math.floor(300000 / this.workloadOpts.machines.length), MIN_QUERY_INTERVAL), MAX_QUERY_INTERVAL),
      lastDayAggrStatus: Math.min(Math.max(Math.floor(300000 / this.workloadOpts.machines.length), MIN_QUERY_INTERVAL), MAX_QUERY_INTERVAL)
    };

    this.queryPhases = {
      lastWeekStates: Math.floor(Math.random() * MAX_QUERY_INTERVAL),
      lastDayAlarms: Math.floor(Math.random() * MAX_QUERY_INTERVAL),
      lasWeekMachineEnergy: Math.floor(Math.random() * MAX_QUERY_INTERVAL),
      lastMonthCountersDifference: Math.floor(Math.random() * MAX_QUERY_INTERVAL),
      oldSetup: Math.floor(Math.random() * MAX_QUERY_INTERVAL),
      lastDayAggrStatus: Math.floor(Math.random() * MAX_QUERY_INTERVAL)
    };

    this.queryPhases = {
      lastWeekStates: Math.floor(Math.random() * 30000),
      lastDayAlarms: Math.floor(Math.random() * 30000),
      lasWeekMachineEnergy: Math.floor(Math.random() * 30000),
      lastMonthCountersDifference: Math.floor(Math.random() * 30000),
      oldSetup: Math.floor(Math.random() * 30000),
      lastDayAggrStatus: Math.floor(Math.random() * 30000)
    };

    this.queryMethods = {
      lastWeekStates: this.lastWeekStates.bind(this),
      lastDayAlarms: this.lastDayAlarms.bind(this),
      lasWeekMachineEnergy: this.lasWeekMachineEnergy.bind(this),
      lastMonthCountersDifference: this.lastMonthCountersDifference.bind(this),
      oldSetup: this.oldSetup.bind(this),
      lastDayAggrStatus: this.lastDayAggrStatus.bind(this)
    };
  }

  sample(id, groupName, absDate) {
    return {
      type: this.sampleTypes[groupName],
      value: this.sampleMethods[groupName](id, absDate)
    };
  }

  statusSample(id, absDate) {
    return {
      deviceType: this.workloadOpts.machineTypeId,
      device: id,
      time: absDate,
      status: {
        time: absDate,
        value: {
          current: Math.round(Math.random() * 100000),
          motors: [
            {
              torque: Math.round(Math.random() * 10000),
              current: Math.round(Math.random() * 6000)
            },
            {
              torque: Math.round(Math.random() * 10000),
              current: Math.round(Math.random() * 6000)
            },
            {
              torque: Math.round(Math.random() * 10000),
              current: Math.round(Math.random() * 6000)
            },
            {
              torque: Math.round(Math.random() * 10000),
              current: Math.round(Math.random() * 6000)
            },
            {
              torque: Math.round(Math.random() * 10000),
              current: Math.round(Math.random() * 6000)
            },
            {
              torque: Math.round(Math.random() * 10000),
              current: Math.round(Math.random() * 6000)
            },
            {
              torque: Math.round(Math.random() * 10000),
              current: Math.round(Math.random() * 6000)
            },
            {
              torque: Math.round(Math.random() * 10000),
              current: Math.round(Math.random() * 6000)
            },
            {
              torque: Math.round(Math.random() * 10000),
              current: Math.round(Math.random() * 6000)
            },
            {
              torque: Math.round(Math.random() * 10000),
              current: Math.round(Math.random() * 6000)
            },
            {
              torque: Math.round(Math.random() * 10000),
              current: Math.round(Math.random() * 6000)
            },
            {
              torque: Math.round(Math.random() * 10000),
              current: Math.round(Math.random() * 6000)
            }
          ],
          tools: [
            {
              temperature: Math.round(Math.random() * 30) + 90
            },
            {
              temperature: Math.round(Math.random() * 30) + 90
            },
            {
              temperature: Math.round(Math.random() * 30) + 90
            },
            {
              temperature: Math.round(Math.random() * 30) + 90
            },
            {
              temperature: Math.round(Math.random() * 30) + 90
            },
            {
              temperature: Math.round(Math.random() * 30) + 90
            },
            {
              temperature: Math.round(Math.random() * 30) + 90
            },
            {
              temperature: Math.round(Math.random() * 30) + 90
            },
            {
              temperature: Math.round(Math.random() * 30) + 90
            },
            {
              temperature: Math.round(Math.random() * 30) + 90
            },
            {
              temperature: Math.round(Math.random() * 30) + 90
            },
            {
              temperature: Math.round(Math.random() * 30) + 90
            },
            {
              temperature: Math.round(Math.random() * 30) + 90
            },
            {
              temperature: Math.round(Math.random() * 30) + 90
            },
            {
              temperature: Math.round(Math.random() * 30) + 90
            },
            {
              temperature: Math.round(Math.random() * 30) + 90
            },
            {
              temperature: Math.round(Math.random() * 30) + 90
            },
            {
              temperature: Math.round(Math.random() * 30) + 90
            },
            {
              temperature: Math.round(Math.random() * 30) + 90
            },
            {
              temperature: Math.round(Math.random() * 30) + 90
            },
            {
              temperature: Math.round(Math.random() * 30) + 90
            },
            {
              temperature: Math.round(Math.random() * 30) + 90
            },
            {
              temperature: Math.round(Math.random() * 30) + 90
            },
            {
              temperature: Math.round(Math.random() * 30) + 90
            }
          ]
        }
      }
    };
  }

  stateSample(id, absDate) {
    return {
      id: uuidv4(),
      deviceType: this.workloadOpts.machineTypeId,
      device: id,
      startTime: absDate,
      endTime: new Date(absDate.getTime() + Math.round(Math.random() * 300 + 60) * 1000),
      value: Math.random() > 0.5 ? "WORKING" : "IDLE"
    };
  }

  countersSample(id, absDate) {
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
            },
            {
              cycles: Math.round(Math.random() * 10000)
            },
            {
              cycles: Math.round(Math.random() * 10000)
            }
          ],
          waterPumps: [
            {
              workingTime: Math.round(Math.random() * 1000000),
            },
            {
              workingTime: Math.round(Math.random() * 1000000),
            },
            {
              workingTime: Math.round(Math.random() * 1000000),
            },
            {
              workingTime: Math.round(Math.random() * 1000000),
            }
          ],
          axis: [
            {
              workingTime: Math.round(Math.random() * 1000000),
            },
            {
              workingTime: Math.round(Math.random() * 1000000),
            },
            {
              workingTime: Math.round(Math.random() * 1000000),
            },
            {
              workingTime: Math.round(Math.random() * 1000000),
            },
            {
              workingTime: Math.round(Math.random() * 1000000),
            },
            {
              workingTime: Math.round(Math.random() * 1000000),
            },
            {
              workingTime: Math.round(Math.random() * 1000000),
            },
            {
              workingTime: Math.round(Math.random() * 1000000),
            },
            {
              workingTime: Math.round(Math.random() * 1000000),
            },
            {
              workingTime: Math.round(Math.random() * 1000000),
            },
            {
              workingTime: Math.round(Math.random() * 1000000),
            },
            {
              workingTime: Math.round(Math.random() * 1000000),
            }
          ],
          tools: [
            {
              failures: Math.round(Math.random() * 100),
              workingTime: Math.round(Math.random() * 1000000),
              lengthProcessed: Math.random() * 1000000
            },
            {
              failures: Math.round(Math.random() * 100),
              workingTime: Math.round(Math.random() * 1000000),
              lengthProcessed: Math.random() * 1000000
            },
            {
              failures: Math.round(Math.random() * 100),
              workingTime: Math.round(Math.random() * 1000000),
              lengthProcessed: Math.random() * 1000000
            },
            {
              failures: Math.round(Math.random() * 100),
              workingTime: Math.round(Math.random() * 1000000),
              lengthProcessed: Math.random() * 1000000
            },
            {
              failures: Math.round(Math.random() * 100),
              workingTime: Math.round(Math.random() * 1000000),
              lengthProcessed: Math.random() * 1000000
            },
            {
              failures: Math.round(Math.random() * 100),
              workingTime: Math.round(Math.random() * 1000000),
              lengthProcessed: Math.random() * 1000000
            },
            {
              failures: Math.round(Math.random() * 100),
              workingTime: Math.round(Math.random() * 1000000),
              lengthProcessed: Math.random() * 1000000
            },
            {
              failures: Math.round(Math.random() * 100),
              workingTime: Math.round(Math.random() * 1000000),
              lengthProcessed: Math.random() * 1000000
            },
            {
              failures: Math.round(Math.random() * 100),
              workingTime: Math.round(Math.random() * 1000000),
              lengthProcessed: Math.random() * 1000000
            },
            {
              failures: Math.round(Math.random() * 100),
              workingTime: Math.round(Math.random() * 1000000),
              lengthProcessed: Math.random() * 1000000
            },
            {
              failures: Math.round(Math.random() * 100),
              workingTime: Math.round(Math.random() * 1000000),
              lengthProcessed: Math.random() * 1000000
            },
            {
              failures: Math.round(Math.random() * 100),
              workingTime: Math.round(Math.random() * 1000000),
              lengthProcessed: Math.random() * 1000000
            },
            {
              failures: Math.round(Math.random() * 100),
              workingTime: Math.round(Math.random() * 1000000),
              lengthProcessed: Math.random() * 1000000
            },
            {
              failures: Math.round(Math.random() * 100),
              workingTime: Math.round(Math.random() * 1000000),
              lengthProcessed: Math.random() * 1000000
            },
            {
              failures: Math.round(Math.random() * 100),
              workingTime: Math.round(Math.random() * 1000000),
              lengthProcessed: Math.random() * 1000000
            },
            {
              failures: Math.round(Math.random() * 100),
              workingTime: Math.round(Math.random() * 1000000),
              lengthProcessed: Math.random() * 1000000
            },
            {
              failures: Math.round(Math.random() * 100),
              workingTime: Math.round(Math.random() * 1000000),
              lengthProcessed: Math.random() * 1000000
            },
            {
              failures: Math.round(Math.random() * 100),
              workingTime: Math.round(Math.random() * 1000000),
              lengthProcessed: Math.random() * 1000000
            },
            {
              failures: Math.round(Math.random() * 100),
              workingTime: Math.round(Math.random() * 1000000),
              lengthProcessed: Math.random() * 1000000
            },
            {
              failures: Math.round(Math.random() * 100),
              workingTime: Math.round(Math.random() * 1000000),
              lengthProcessed: Math.random() * 1000000
            },
            {
              failures: Math.round(Math.random() * 100),
              workingTime: Math.round(Math.random() * 1000000),
              lengthProcessed: Math.random() * 1000000
            },
            {
              failures: Math.round(Math.random() * 100),
              workingTime: Math.round(Math.random() * 1000000),
              lengthProcessed: Math.random() * 1000000
            },
            {
              failures: Math.round(Math.random() * 100),
              workingTime: Math.round(Math.random() * 1000000),
              lengthProcessed: Math.random() * 1000000
            },
            {
              failures: Math.round(Math.random() * 100),
              workingTime: Math.round(Math.random() * 1000000),
              lengthProcessed: Math.random() * 1000000
            }
          ],
          motors: [
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

  setupSample(id, absDate) {
    return {
      deviceType: this.workloadOpts.machineTypeId,
      device: id,
      time: absDate,
      setup: {
        time: absDate,
        value: {
          partThickness: Math.round(Math.random() * 50),
          edgeHeight: Math.round(Math.random() * 25),
          edgeAngle: Math.round(Math.random() * 45),
          speed: Math.round(Math.random() * 20),
          partType: Math.round(Math.random() * 5).toString(),
          partCode: Math.round(Math.random() * 10000).toString(),
          batchCode: Math.round(Math.random() * 10000).toString(),
          tools: [
            {
              pressure: Math.round(Math.random() * 10000)
            },
            {
              pressure: Math.round(Math.random() * 10000)
            },
            {
              pressure: Math.round(Math.random() * 10000)
            },
            {
              pressure: Math.round(Math.random() * 10000)
            },
            {
              pressure: Math.round(Math.random() * 10000)
            },
            {
              pressure: Math.round(Math.random() * 10000)
            },
            {
              pressure: Math.round(Math.random() * 10000)
            },
            {
              pressure: Math.round(Math.random() * 10000)
            },
            {
              pressure: Math.round(Math.random() * 10000)
            },
            {
              pressure: Math.round(Math.random() * 10000)
            },
            {
              pressure: Math.round(Math.random() * 10000)
            },
            {
              pressure: Math.round(Math.random() * 10000)
            },
            {
              pressure: Math.round(Math.random() * 10000)
            },
            {
              pressure: Math.round(Math.random() * 10000)
            },
            {
              pressure: Math.round(Math.random() * 10000)
            },
            {
              pressure: Math.round(Math.random() * 10000)
            },
            {
              pressure: Math.round(Math.random() * 10000)
            },
            {
              pressure: Math.round(Math.random() * 10000)
            },
            {
              pressure: Math.round(Math.random() * 10000)
            },
            {
              pressure: Math.round(Math.random() * 10000)
            },
            {
              pressure: Math.round(Math.random() * 10000)
            },
            {
              pressure: Math.round(Math.random() * 10000)
            },
            {
              pressure: Math.round(Math.random() * 10000)
            },
            {
              pressure: Math.round(Math.random() * 10000)
            }
          ]
        }
      }
    };
  }

  geoSample(id, absDate) {
    return {
      deviceType: this.workloadOpts.machineTypeId,
      device: id,
      time: absDate,
      geo: {
        time: absDate,
        value: {
          lng: Math.random() * 180,
          lat: Math.random() * 90
        }
      }
    };
  }

  alarmSample(id, absDate) {
    return {
      id: uuidv4(),
      deviceType: this.workloadOpts.machineTypeId,
      device: id,
      startTime: absDate,
      endTime: new Date(absDate.getTime() + Math.round(Math.random() * 300 + 60) * 1000),
      value: Math.ceil(Math.random() * 100).toString()
    };
  }

  query(query, absDate) {
    return this.queryMethods[query](absDate);
  }

  lastWeekStates(absDate) {
    let machineIndex = Math.floor(this.workloadOpts.machines.length * this.workloadOpts.machineUptime * Math.random());

    return {
      name: "LAST_WEEK_STATES",
      type: "INTERVAL_RANGE",
      options: {
        group: "state",
        deviceType: this.workloadOpts.machineTypeId,
        device: this.workloadOpts.machines[machineIndex],
        startTime: new Date(absDate.getTime() - 604800000),
        endTime: absDate
      },
      interval: this.sampleIntervals.state
    };
  }

  lastDayAlarms(absDate) {
    let machineIndex = Math.floor(this.workloadOpts.machines.length * this.workloadOpts.machineUptime * Math.random());

    return {
      name: "LAST_DAY_ALARMS",
      type: "INTERVAL_RANGE",
      options: {
        group: "alarm",
        deviceType: this.workloadOpts.machineTypeId,
        device: this.workloadOpts.machines[machineIndex],
        startTime: new Date(absDate.getTime() - 86400000),
        endTime: absDate
      },
      interval: this.sampleIntervals.alarm
    };
  }

  lasWeekMachineEnergy(absDate) {
    let machineIndex = Math.floor(this.workloadOpts.machines.length * this.workloadOpts.machineUptime * Math.random());

    return {
      name: "LAST_WEEK_MACHINE_ENERGY",
      type: "TIME_COMPLEX_RANGE",
      options: {
        group: "counters",
        select: [ "activeEnergyConsumed", "reactiveEnergyProduced" ],
        deviceType: this.workloadOpts.machineTypeId,
        device: this.workloadOpts.machines[machineIndex],
        startTime: new Date(absDate.getTime() - 604800000),
        endTime: absDate
      },
      interval: this.sampleIntervals.counters
    };
  }

  lastMonthCountersDifference(absDate) {
    let machineIndex = Math.floor(this.workloadOpts.machines.length * this.workloadOpts.machineUptime * Math.random());

    return {
      name: "LAST_MONTH_COUNTERS_DIFFERENCE",
      type: "TIME_COMPLEX_DIFFERENCE",
      options: {
        group: "counters",
        deviceType: this.workloadOpts.machineTypeId,
        device: this.workloadOpts.machines[machineIndex],
        startTime: new Date(absDate.getTime() - 2073600000),
        endTime: absDate
      },
      interval: this.sampleIntervals.counters
    };
  }

  oldSetup(absDate) {
    let machineIndex = Math.floor(this.workloadOpts.machines.length * this.workloadOpts.machineUptime * Math.random());
    let yearTime = moment(absDate).startOf("year").valueOf();
    let nowTime = absDate.getTime();

    return {
      name: "OLD_SETUP",
      type: "TIME_COMPLEX_LAST_BEFORE",
      options: {
        group: "setup",
        deviceType: this.workloadOpts.machineTypeId,
        device: this.workloadOpts.machines[machineIndex],
        time: new Date(yearTime + (nowTime - yearTime) * Math.random())
      },
      interval: this.sampleIntervals.setup
    };
  }

  lastDayAggrStatus(absDate) {
    let machineIndex = Math.floor(this.workloadOpts.machines.length * this.workloadOpts.machineUptime * Math.random());

    return {
      name: "LAST_TWO_WEEKS_STATUS_AGGR",
      type: "TIME_COMPLEX_RANGE_BUCKET_AVG",
      options: {
        group: "status",
        select: ["current"],
        deviceType: this.workloadOpts.machineTypeId,
        device: this.workloadOpts.machines[machineIndex],
        startTime: new Date(absDate.getTime() - 1209600000),
        endTime: absDate,
        buckets: 256
      },
      interval: this.sampleIntervals.status
    };
  }

};
