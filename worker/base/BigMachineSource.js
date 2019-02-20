"use strict";

const uuidv4 = require('uuid/v4');
const moment = require('moment');

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
      setup: 300000,
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
      lastWeekStates: Math.floor(28800000 / this.workloadOpts.machines.length),
      lastWeekSetups: Math.floor(28800000 / this.workloadOpts.machines.length),
      lastDayAlarms: Math.floor(28800000 / this.workloadOpts.machines.length),
      lastMonthMachineEnergy: Math.floor(28800000 / this.workloadOpts.machines.length),
      lastHourExtrusion: Math.floor(28800000 / this.workloadOpts.machines.length),
      lastDayAggrExtrusion: Math.floor(28800000 / this.workloadOpts.machines.length),
      lastHourCooling: Math.floor(28800000 / this.workloadOpts.machines.length),
      lastDayAggrCooling: Math.floor(28800000 / this.workloadOpts.machines.length),
      lastHourMotion: Math.floor(28800000 / this.workloadOpts.machines.length),
      lastDayAggrMotion: Math.floor(28800000 / this.workloadOpts.machines.length),
      thisYearMonthlyCountersDifference: Math.floor(28800000 / this.workloadOpts.machines.length),
      oldStatus: Math.floor(604800000 / this.workloadOpts.machines.length),
      topTenMachinesLastDayWorkingTime: 86400000,
      topTenMachinesLastDayAlarms: 86400000
    };

    this.queryMethods = {
      lastWeekStates: this.lastWeekStates.bind(this),
      lastWeekSetups: this.lastWeekSetups.bind(this),
      lastDayAlarms: this.lastDayAlarms.bind(this),
      lastMonthMachineEnergy: this.lastMonthMachineEnergy.bind(this),
      lastHourExtrusion: this.lastHourExtrusion.bind(this),
      lastDayAggrExtrusion: this.lastDayAggrExtrusion.bind(this),
      lastHourCooling: this.lastHourCooling.bind(this),
      lastDayAggrCooling: this.lastDayAggrCooling.bind(this),
      lastHourMotion: this.lastHourMotion.bind(this),
      lastDayAggrMotion: this.lastDayAggrMotion.bind(this),
      thisYearMonthlyCountersDifference: this.thisYearMonthlyCountersDifference.bind(this),
      oldStatus: this.oldStatus.bind(this),
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
    }
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
              temperature: Math.round() * 75
            },
            {
              temperature: Math.round() * 75
            },
            {
              temperature: Math.round() * 75
            },
            {
              temperature: Math.round() * 75
            },
            {
              temperature: Math.round() * 75
            },
            {
              temperature: Math.round() * 75
            },
            {
              temperature: Math.round() * 75
            },
            {
              temperature: Math.round() * 75
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
        groups: ["state"],
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
        groups: ["setup"],
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
        groups: ["alarm"],
        deviceType: this.workloadOpts.machineTypeId,
        device: this.workloadOpts.machines[machineIndex],
        startTime: new Date(absDate.getTime() - 86400000),
        endTime: absDate
      },
      interval: this.sampleIntervals.alarm
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
        groups: ["extrusion"],
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
        groups: ["extrusion"],
        select: {
          extrusion: ["screwRpm"]
        },
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
        groups: ["motion"],
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
        groups: ["motion"],
        select: {
          motion: [
            "rpm",
            "phase",
            "topBearingVibration",
            "bottomBearingVibration"
          ]
        },
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
        groups: ["cooling"],
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
        groups: ["cooling"],
        select: {
          cooling: ["oilPressure"]
        },
        deviceType: this.workloadOpts.machineTypeId,
        device: this.workloadOpts.machines[machineIndex],
        startTime: new Date(absDate.getTime() - 86400000),
        endTime: absDate,
        buckets: 1024
      },
      interval: this.sampleIntervals.cooling
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
      },
      interval: this.sampleIntervals.counters
    };
  }

  oldStatus(absDate) {
    let machineIndex = Math.floor(this.workloadOpts.machines.length * this.workloadOpts.machineUptime * Math.random());
    let yearTime = moment(absDate).startOf('year').valueOf();
    let nowTime = absDate.getTime();

    return {
      name: "OLD_MACHINE_STATUS",
      type: "TIME_COMPLEX_LAST_BEFORE",
      options: {
        groups: ["status"],
        deviceType: this.workloadOpts.machineTypeId,
        device: this.workloadOpts.machines[machineIndex],
        time: new Date(yearTime + (nowTime - yearTime) * Math.random())
      },
      interval: this.sampleIntervals.status
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
      },
      interval: this.sampleIntervals.counters
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
      },
      interval: this.sampleIntervals.alarm
    };
  }

};
