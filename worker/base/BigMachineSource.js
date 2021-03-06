"use strict";

const uuidv4 = require("uuid/v4");
const moment = require("moment");

const MIN_QUERY_INTERVAL = 15000;
const MAX_QUERY_INTERVAL = 30000;

module.exports = class BigMachineSource {

  constructor(workloadOpts) {
    this.workloadOpts = workloadOpts;

    this.sampleTypes = {
      state: "INTERVAL",
      status: "TIME_COMPLEX",
      counters: "TIME_COMPLEX",
      setup: "INTERVAL",
      cooling: "TIME_COMPLEX",
      extrusion: "TIME_COMPLEX",
      motion: "TIME_COMPLEX",
      alarm: "INTERVAL"
    };

    this.sampleIntervals = {
      state: 3600000,
      status: 10000,
      counters: 60000,
      setup: 28800000,
      cooling: 1000,
      extrusion: 1000,
      motion: 1000,
      alarm: 1800000
    };

    this.sampleMethods = {
      state: this.stateSample.bind(this),
      status: this.statusSample.bind(this),
      counters: this.countersSample.bind(this),
      setup: this.setupSample.bind(this),
      cooling: this.coolingSample.bind(this),
      extrusion: this.extrusionSample.bind(this),
      motion: this.motionSample.bind(this),
      alarm: this.alarmSample.bind(this)
    };

    this.queryIntervals = {
      lastWeekStates: Math.min(Math.max(Math.floor(28800000 / this.workloadOpts.machines.length), MIN_QUERY_INTERVAL), MAX_QUERY_INTERVAL),
      lastWeekSetups: Math.min(Math.max(Math.floor(28800000 / this.workloadOpts.machines.length), MIN_QUERY_INTERVAL), MAX_QUERY_INTERVAL),
      lastDayAlarms: Math.min(Math.max(Math.floor(28800000 / this.workloadOpts.machines.length), MIN_QUERY_INTERVAL), MAX_QUERY_INTERVAL),
      lasWeekMachineEnergy: Math.min(Math.max(Math.floor(28800000 / this.workloadOpts.machines.length), MIN_QUERY_INTERVAL), MAX_QUERY_INTERVAL),
      lastHourExtrusion: Math.min(Math.max(Math.floor(28800000 / this.workloadOpts.machines.length), MIN_QUERY_INTERVAL), MAX_QUERY_INTERVAL),
      lastDayAggrExtrusion: Math.min(Math.max(Math.floor(28800000 / this.workloadOpts.machines.length), MIN_QUERY_INTERVAL), MAX_QUERY_INTERVAL),
      lastHourCooling: Math.min(Math.max(Math.floor(28800000 / this.workloadOpts.machines.length), MIN_QUERY_INTERVAL), MAX_QUERY_INTERVAL),
      lastDayAggrCooling: Math.min(Math.max(Math.floor(28800000 / this.workloadOpts.machines.length), MIN_QUERY_INTERVAL), MAX_QUERY_INTERVAL),
      lastHourMotion: Math.min(Math.max(Math.floor(28800000 / this.workloadOpts.machines.length), MIN_QUERY_INTERVAL), MAX_QUERY_INTERVAL),
      lastDayAggrMotion: Math.min(Math.max(Math.floor(28800000 / this.workloadOpts.machines.length), MIN_QUERY_INTERVAL), MAX_QUERY_INTERVAL),
      lastMonthCountersDifference: Math.min(Math.max(Math.floor(28800000 / this.workloadOpts.machines.length), MIN_QUERY_INTERVAL), MAX_QUERY_INTERVAL),
      oldStatus: Math.min(Math.max(Math.floor(86400000 / this.workloadOpts.machines.length), MIN_QUERY_INTERVAL), MAX_QUERY_INTERVAL)
    };

    this.queryPhases = {
      lastWeekStates: Math.floor(Math.random() * MAX_QUERY_INTERVAL),
      lastWeekSetups: Math.floor(Math.random() * MAX_QUERY_INTERVAL),
      lastDayAlarms: Math.floor(Math.random() * MAX_QUERY_INTERVAL),
      lasWeekMachineEnergy: Math.floor(Math.random() * MAX_QUERY_INTERVAL),
      lastHourExtrusion: Math.floor(Math.random() * MAX_QUERY_INTERVAL),
      lastDayAggrExtrusion: Math.floor(Math.random() * MAX_QUERY_INTERVAL),
      lastHourCooling: Math.floor(Math.random() * MAX_QUERY_INTERVAL),
      lastDayAggrCooling: Math.floor(Math.random() * MAX_QUERY_INTERVAL),
      lastHourMotion: Math.floor(Math.random() * MAX_QUERY_INTERVAL),
      lastDayAggrMotion: Math.floor(Math.random() * MAX_QUERY_INTERVAL),
      lastMonthCountersDifference: Math.floor(Math.random() * MAX_QUERY_INTERVAL),
      oldStatus: Math.floor(Math.random() * MAX_QUERY_INTERVAL)
    };

    this.queryMethods = {
      lastWeekStates: this.lastWeekStates.bind(this),
      lastWeekSetups: this.lastWeekSetups.bind(this),
      lastDayAlarms: this.lastDayAlarms.bind(this),
      lasWeekMachineEnergy: this.lasWeekMachineEnergy.bind(this),
      lastHourExtrusion: this.lastHourExtrusion.bind(this),
      lastDayAggrExtrusion: this.lastDayAggrExtrusion.bind(this),
      lastHourCooling: this.lastHourCooling.bind(this),
      lastDayAggrCooling: this.lastDayAggrCooling.bind(this),
      lastHourMotion: this.lastHourMotion.bind(this),
      lastDayAggrMotion: this.lastDayAggrMotion.bind(this),
      lastMonthCountersDifference: this.lastMonthCountersDifference.bind(this),
      oldStatus: this.oldStatus.bind(this)
    };
  }

  sample(id, groupName, absDate) {
    return {
      type: this.sampleTypes[groupName],
      value: this.sampleMethods[groupName](id, absDate)
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

  statusSample(id, absDate) {
    return {
      deviceType: this.workloadOpts.machineTypeId,
      device: id,
      time: absDate,
      status: {
        time: absDate,
        value: {
          plcCommunication: Math.random() > 0.5,
          maintenanceMode: Math.random() > 0.5,
          missingPart: Math.random() > 0.5,
          externalAlarm: Math.random() > 0.5
        }
      }
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
          totalUpstreamWaitingTime: Math.round(Math.random() * 1000000),
          partialUpstreamWaitingTime: Math.round(Math.random() * 1000000),
          totalWorkedTime: Math.round(Math.random() * 1000000),
          partialWorkedTime: Math.round(Math.random() * 1000000),
          totalDownstreamWaitingTime: Math.round(Math.random() * 1000000),
          partialDownstreamWaitingTime: Math.round(Math.random() * 1000000),
          totalCompliantParts: Math.round(Math.random() * 1000000),
          partialCompliantParts: Math.round(Math.random() * 1000000),
          totalRejectedParts: Math.round(Math.random() * 1000000),
          partialRejectedParts: Math.round(Math.random() * 1000000),
          totalWastedParts: Math.round(Math.random() * 1000000),
          partialWastedParts: Math.round(Math.random() * 1000000),
          punches: [
            {
              hits: Math.round(Math.random() * 10000)
            },
            {
              hits: Math.round(Math.random() * 10000)
            },
            {
              hits: Math.round(Math.random() * 10000)
            },
            {
              hits: Math.round(Math.random() * 10000)
            },
            {
              hits: Math.round(Math.random() * 10000)
            },
            {
              hits: Math.round(Math.random() * 10000)
            },
            {
              hits: Math.round(Math.random() * 10000)
            },
            {
              hits: Math.round(Math.random() * 10000)
            },
            {
              hits: Math.round(Math.random() * 10000)
            },
            {
              hits: Math.round(Math.random() * 10000)
            },
            {
              hits: Math.round(Math.random() * 10000)
            },
            {
              hits: Math.round(Math.random() * 10000)
            },
            {
              hits: Math.round(Math.random() * 10000)
            },
            {
              hits: Math.round(Math.random() * 10000)
            },
            {
              hits: Math.round(Math.random() * 10000)
            },
            {
              hits: Math.round(Math.random() * 10000)
            }
          ]
        }
      }
    };
  }

  setupSample(id, absDate) {
    return {
      id: uuidv4(),
      deviceType: this.workloadOpts.machineTypeId,
      device: id,
      startTime: absDate,
      endTime: new Date(absDate.getTime() + Math.round(Math.random() * 300 + 60) * 1000),
      value: {
        partType: Math.round(Math.random() * 5).toString(),
        partCode: Math.round(Math.random() * 10000).toString(),
        batchCode: Math.round(Math.random() * 10000).toString(),
        cooling: {
          oilPressure: Math.random() * 10000,
          fans: [
            {
              speed: Math.round(Math.random() * 100)
            },
            {
              speed: Math.round(Math.random() * 100)
            },
            {
              speed: Math.round(Math.random() * 100)
            },
            {
              speed: Math.round(Math.random() * 100)
            }
          ]
        },
        motion: {
          rpm: Math.random() * 100,
          phase: Math.random() * 100,
        },
        extrusion: {
          preheatTemperature: Math.round(Math.random() * 80),
          melts: [
            {
              temperature: Math.round(Math.random() * 150)
            },
            {
              temperature: Math.round(Math.random() * 150)
            },
            {
              temperature: Math.round(Math.random() * 150)
            },
            {
              temperature: Math.round(Math.random() * 150)
            }
          ],
          screwRpm: Math.round(Math.random() * 45),
          nozzleReheat: Math.round(Math.random() * 150),
        },
        punches: [
          {
            excluded: Math.random() > 0.5,
            advance: Math.random() * 1000,
            reheatTemperature: Math.round(Math.random() * 120)
          },
          {
            excluded: Math.random() > 0.5,
            advance: Math.random() * 1000,
            reheatTemperature: Math.round(Math.random() * 120)
          },
          {
            excluded: Math.random() > 0.5,
            advance: Math.random() * 1000,
            reheatTemperature: Math.round(Math.random() * 120)
          },
          {
            excluded: Math.random() > 0.5,
            advance: Math.random() * 1000,
            reheatTemperature: Math.round(Math.random() * 120)
          },
          {
            excluded: Math.random() > 0.5,
            advance: Math.random() * 1000,
            reheatTemperature: Math.round(Math.random() * 120)
          },
          {
            excluded: Math.random() > 0.5,
            advance: Math.random() * 1000,
            reheatTemperature: Math.round(Math.random() * 120)
          },
          {
            excluded: Math.random() > 0.5,
            advance: Math.random() * 1000,
            reheatTemperature: Math.round(Math.random() * 120)
          },
          {
            excluded: Math.random() > 0.5,
            advance: Math.random() * 1000,
            reheatTemperature: Math.round(Math.random() * 120)
          },
          {
            excluded: Math.random() > 0.5,
            advance: Math.random() * 1000,
            reheatTemperature: Math.round(Math.random() * 120)
          },
          {
            excluded: Math.random() > 0.5,
            advance: Math.random() * 1000,
            reheatTemperature: Math.round(Math.random() * 120)
          },
          {
            excluded: Math.random() > 0.5,
            advance: Math.random() * 1000,
            reheatTemperature: Math.round(Math.random() * 120)
          },
          {
            excluded: Math.random() > 0.5,
            advance: Math.random() * 1000,
            reheatTemperature: Math.round(Math.random() * 120)
          },
          {
            excluded: Math.random() > 0.5,
            advance: Math.random() * 1000,
            reheatTemperature: Math.round(Math.random() * 120)
          },
          {
            excluded: Math.random() > 0.5,
            advance: Math.random() * 1000,
            reheatTemperature: Math.round(Math.random() * 120)
          },
          {
            excluded: Math.random() > 0.5,
            advance: Math.random() * 1000,
            reheatTemperature: Math.round(Math.random() * 120)
          },
          {
            excluded: Math.random() > 0.5,
            advance: Math.random() * 1000,
            reheatTemperature: Math.round(Math.random() * 120)
          }
        ]
      }
    };
  }

  coolingSample(id, absDate) {
    return {
      deviceType: this.workloadOpts.machineTypeId,
      device: id,
      time: absDate,
      cooling: {
        time: absDate,
        value: {
          oilPressure: Math.random() * 10000,
          probes: [
            {
              temperature: Math.random() * 75
            },
            {
              temperature: Math.random() * 75
            },
            {
              temperature: Math.random() * 75
            },
            {
              temperature: Math.random() * 75
            },
            {
              temperature: Math.random() * 75
            },
            {
              temperature: Math.random() * 75
            },
            {
              temperature: Math.random() * 75
            },
            {
              temperature: Math.random() * 75
            }
          ]
        }
      }
    };
  }

  extrusionSample(id, absDate) {
    return {
      deviceType: this.workloadOpts.machineTypeId,
      device: id,
      time: absDate,
      extrusion: {
        time: absDate,
        value: {
          preheatTemperature: Math.round(Math.random() * 80),
          melts: [
            {
              temperature: Math.round(Math.random() * 150),
              pressure: Math.round(Math.random() * 50)
            },
            {
              temperature: Math.round(Math.random() * 150),
              pressure: Math.round(Math.random() * 50)
            },
            {
              temperature: Math.round(Math.random() * 150),
              pressure: Math.round(Math.random() * 50)
            },
            {
              temperature: Math.round(Math.random() * 150),
              pressure: Math.round(Math.random() * 50)
            }
          ],
          screwRpm: Math.round(Math.random() * 45),
          nozzleReheat: Math.round(Math.random() * 150),
        }
      }
    };
  }

  motionSample(id, absDate) {
    return {
      deviceType: this.workloadOpts.machineTypeId,
      device: id,
      time: absDate,
      motion: {
        time: absDate,
        value: {
          rpm: Math.random() * 100,
          phase: Math.random() * 100,
          topBearingVibration: Math.random() * 100,
          bottomBearingVibration: Math.random() * 100,
          inverters: [
            {
              percent: Math.round(Math.random() * 100),
              temperature: Math.round(Math.random() * 60)
            },
            {
              percent: Math.round(Math.random() * 100),
              temperature: Math.round(Math.random() * 60)
            },
            {
              percent: Math.round(Math.random() * 100),
              temperature: Math.round(Math.random() * 60)
            },
            {
              percent: Math.round(Math.random() * 100),
              temperature: Math.round(Math.random() * 60)
            }
          ]
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

  lastWeekSetups(absDate) {
    let machineIndex = Math.floor(this.workloadOpts.machines.length * this.workloadOpts.machineUptime * Math.random());

    return {
      name: "LAST_WEEK_SETUPS",
      type: "INTERVAL_RANGE",
      options: {
        group: "setup",
        deviceType: this.workloadOpts.machineTypeId,
        device: this.workloadOpts.machines[machineIndex],
        startTime: new Date(absDate.getTime() - 604800000),
        endTime: absDate
      },
      interval: this.sampleIntervals.setup
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

  lastHourExtrusion(absDate) {
    let machineIndex = Math.floor(this.workloadOpts.machines.length * this.workloadOpts.machineUptime * Math.random());

    return {
      name: "LAST_HOUR_EXTRUSION",
      type: "TIME_COMPLEX_RANGE",
      options: {
        group: "extrusion",
        deviceType: this.workloadOpts.machineTypeId,
        device: this.workloadOpts.machines[machineIndex],
        startTime: new Date(absDate.getTime() - 3600000),
        endTime: absDate
      },
      interval: this.sampleIntervals.extrusion
    };
  }

  lastDayAggrExtrusion(absDate) {
    let machineIndex = Math.floor(this.workloadOpts.machines.length * this.workloadOpts.machineUptime * Math.random());

    return {
      name: "LAST_DAY_EXTRUSION_AGGR",
      type: "TIME_COMPLEX_RANGE_BUCKET_AVG",
      options: {
        group: "extrusion",
        select: ["screwRpm"],
        deviceType: this.workloadOpts.machineTypeId,
        device: this.workloadOpts.machines[machineIndex],
        startTime: new Date(absDate.getTime() - 86400000),
        endTime: absDate,
        buckets: 64
      },
      interval: this.sampleIntervals.extrusion
    };
  }

  lastHourMotion(absDate) {
    let machineIndex = Math.floor(this.workloadOpts.machines.length * this.workloadOpts.machineUptime * Math.random());

    return {
      name: "LAST_HOUR_MOTION",
      type: "TIME_COMPLEX_RANGE",
      options: {
        group: "motion",
        deviceType: this.workloadOpts.machineTypeId,
        device: this.workloadOpts.machines[machineIndex],
        startTime: new Date(absDate.getTime() - 3600000),
        endTime: absDate
      },
      interval: this.sampleIntervals.motion
    };
  }

  lastDayAggrMotion(absDate) {
    let machineIndex = Math.floor(this.workloadOpts.machines.length * this.workloadOpts.machineUptime * Math.random());

    return {
      name: "LAST_DAY_MOTION_AGGR",
      type: "TIME_COMPLEX_RANGE_BUCKET_AVG",
      options: {
        group: "motion",
        select: [ "rpm", "phase", "topBearingVibration", "bottomBearingVibration" ],
        deviceType: this.workloadOpts.machineTypeId,
        device: this.workloadOpts.machines[machineIndex],
        startTime: new Date(absDate.getTime() - 86400000),
        endTime: absDate,
        buckets: 1024
      },
      interval: this.sampleIntervals.motion
    };
  }


  lastHourCooling(absDate) {
    let machineIndex = Math.floor(this.workloadOpts.machines.length * this.workloadOpts.machineUptime * Math.random());

    return {
      name: "LAST_HOUR_COOLING",
      type: "TIME_COMPLEX_RANGE",
      options: {
        group: "cooling",
        deviceType: this.workloadOpts.machineTypeId,
        device: this.workloadOpts.machines[machineIndex],
        startTime: new Date(absDate.getTime() - 3600000),
        endTime: absDate
      },
      interval: this.sampleIntervals.cooling
    };
  }

  lastDayAggrCooling(absDate) {
    let machineIndex = Math.floor(this.workloadOpts.machines.length * this.workloadOpts.machineUptime * Math.random());

    return {
      name: "LAST_DAY_COOLING_AGGR",
      type: "TIME_COMPLEX_RANGE_BUCKET_AVG",
      options: {
        group: "cooling",
        select: ["oilPressure"],
        deviceType: this.workloadOpts.machineTypeId,
        device: this.workloadOpts.machines[machineIndex],
        startTime: new Date(absDate.getTime() - 86400000),
        endTime: absDate,
        buckets: 1024
      },
      interval: this.sampleIntervals.cooling
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
        startTime: new Date(absDate.getTime() - 2592000000),
        endTime: absDate,
      },
      interval: this.sampleIntervals.counters
    };
  }

  oldStatus(absDate) {
    let machineIndex = Math.floor(this.workloadOpts.machines.length * this.workloadOpts.machineUptime * Math.random());
    let yearTime = moment(absDate).startOf("year").valueOf();
    let nowTime = absDate.getTime();

    return {
      name: "OLD_MACHINE_STATUS",
      type: "TIME_COMPLEX_LAST_BEFORE",
      options: {
        group: "status",
        deviceType: this.workloadOpts.machineTypeId,
        device: this.workloadOpts.machines[machineIndex],
        time: new Date(yearTime + (nowTime - yearTime) * Math.random())
      },
      interval: this.sampleIntervals.status
    };
  }

};
