"use strict";

const uuidv4 = require('uuid/v4');
const moment = require('moment');

module.exports = class BigMachineSource {

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

    this.sampleIntervals = {
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

    this.queryIntervals = {
      lastWeekStatus: 0,
      lastDayAlarms: 0,
      lastMonthMachineEnergy: 0,
      lastHourTemperatures: 0,
      lastDayAggrTemperatures: 0,
      monthlyCountersDifference: 0,
      oldSetup: 0,
      topTenMachinesLastDayWorkingTime: 0,
      topTenMachinesLastDayAlarms: 0
    };

    this.queryMethods = {
      lastWeekStatus: this.lastWeekStatus.bind(this),
      lastDayAlarms: this.lastDayAlarms.bind(this),
      lastMonthMachineEnergy: this.lastMonthMachineEnergy.bind(this),
      lastHourTemperatures: this.lastHourTemperatures.bind(this),
      lastDayAggrTemperatures: this.lastDayAggrTemperatures.bind(this),
      monthlyCountersDifference: this.monthlyCountersDifference.bind(this),
      oldSetup: this.oldSetup.bind(this),
      topTenMachinesLastDayWorkingTime: this.topTenMachinesLastDayWorkingTime.bind(this),
      topTenMachinesLastDayAlarms: this.topTenMachinesLastDayAlarms.bind(this)
    };
  }

  sample(id, groupName, absDate) {
    return {
      type: this.sampleTypes[groupName],
      value: this.sampleMethods[groupName](id, absDate)
    }
  }

  statusSample(id, absDate) {
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

  setupSample(id, absDate) {
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

  temperatureProbe1Sample(id, absDate) {
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

  temperatureProbe2Sample(id, absDate) {
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

  lastWeekStatus(absDate) {
    let machineIndex = Math.floor(this.workloadOpts.machines.length * this.workloadOpts.machineUptime * Math.random());

    return {
      name: "LAST_WEEK_STATUS",
      type: "INTERVAL_RANGE",
      options: {
        groups: ["status"],
        deviceType: this.workloadOpts.machineTypeId,
        device: this.workloadOpts.machines[machineIndex],
        startTime: new Date(absDate.getTime() - 604800000),
        endTime: absDate
      }
    };
  }

  lastDayAlarms(absDate) {
    let machineIndex = Math.floor(this.workloadOpts.machines.length * this.workloadOpts.machineUptime * Math.random());

    return {
      name: "LAST_DAY_ALARMS",
      type: "INTERVAL_RANGE",
      options: {
        groups: ["alarm"],
        deviceType: this.workloadOpts.machineTypeId,
        device: this.workloadOpts.machines[machineIndex],
        startTime: new Date(absDate.getTime() - 86400000),
        endTime: absDate
      }
    };
  }

  lastMonthMachineEnergy(absDate) {
    let machineIndex = Math.floor(this.workloadOpts.machines.length * this.workloadOpts.machineUptime * Math.random());

    return {
      name: "LAST_MONTH_MACHINE_ENERGY",
      type: "TIME_COMPLEX_RANGE",
      options: {
        groups: ["counters"],
        select: {
          "counters": [
            "activeEnergyConsumed",
            "reactiveEnergyProduced"
          ]
        },
        deviceType: this.workloadOpts.machineTypeId,
        device: this.workloadOpts.machines[machineIndex],
        startTime: new Date(absDate.getTime() - 2073600000),
        endTime: absDate
      }
    };
  }

  lastHourTemperatures(absDate) {
    let machineIndex = Math.floor(this.workloadOpts.machines.length * this.workloadOpts.machineUptime * Math.random());

    return {
      name: "LAST_HOUR_TEMPERATURES",
      type: "TIME_COMPLEX_RANGE",
      options: {
        groups: ["temperatureProbe1", "temperatureProbe2"],
        deviceType: this.workloadOpts.machineTypeId,
        device: this.workloadOpts.machines[machineIndex],
        startTime: new Date(absDate.getTime() - 3600000),
        endTime: absDate
      }
    };
  }

  lastDayAggrTemperatures(absDate) {
    let machineIndex = Math.floor(this.workloadOpts.machines.length * this.workloadOpts.machineUptime * Math.random());

    return {
      name: "LAST_HOUR_TEMPERATURES",
      type: "TIME_COMPLEX_RANGE_BUCKET_AVG",
      options: {
        groups: ["temperatureProbe1", "temperatureProbe2"],
        deviceType: this.workloadOpts.machineTypeId,
        device: this.workloadOpts.machines[machineIndex],
        startTime: new Date(absDate.getTime() - 86400000),
        endTime: absDate,
        buckets: 1024
      }
    };
  }

  thisYearMonthlyCountersDifference(absDate) {
    let machineIndex = Math.floor(this.workloadOpts.machines.length * this.workloadOpts.machineUptime * Math.random());

    let times = [];
    let absM = moment(absDate);
    let m = moment(absDate).startOf('year');
    let year = m.year();
    while (m.year() === year && m.isBefore(absM)) {
      times.push(m.toDate());
      m.add(1, 'month');
    }

    return {
      name: "THIS_YEAR_MONTHLY_COUNTERS_DIFFERENCE",
      type: "TIME_COMPLEX_DIFFERENCE",
      options: {
        groups: ["counters"],
        deviceType: this.workloadOpts.machineTypeId,
        device: this.workloadOpts.machines[machineIndex],
        times: times
      }
    };
  }

  oldSetup(absDate) {
    let machineIndex = Math.floor(this.workloadOpts.machines.length * this.workloadOpts.machineUptime * Math.random());
    let yearTime = moment(absDate).startOf('year').valueOf();
    let nowTime = absDate.getTime();

    return {
      name: "LAST_MONTH_MACHINE_ENERGY",
      type: "TIME_COMPLEX_LAST_BEFORE",
      options: {
        groups: ["counters"],
        deviceType: this.workloadOpts.machineTypeId,
        device: this.workloadOpts.machines[machineIndex],
        time: new Date(yearTime + (nowTime - yearTime) * Math.random())
      }
    };
  }

  topTenMachinesLastDayWorkingTime(absDate) {
    return {
      name: "TOP_TEN_MACHINES_LAST_DAY_WORKING_TIME",
      type: "TIME_COMPLEX_TOP_DIFFERENCE",
      options: {
        deviceType: this.workloadOpts.machineTypeId,
        groups: ["counters"],
        sort: {"totalWorkedTime": -1},
        limit: 10,
        startTime: new Date(absDate.getTime() - 86400000),
        endTime: absDate
      }
    };
  }

  topTenMachinesLastDayAlarms(absDate) {
    return {
      name: "TOP_TEN_MACHINES_LAST_DAY_ALARMS",
      type: "INTERVAL_TOP_COUNT",
      options: {
        deviceType: this.workloadOpts.machineTypeId,
        groups: ["alarm"],
        limit: 10,
        startTime: new Date(absDate.getTime() - 86400000),
        endTime: absDate
      }
    };
  }

};
