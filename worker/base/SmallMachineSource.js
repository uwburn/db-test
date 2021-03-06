"use strict";

const uuidv4 = require("uuid/v4");
const moment = require("moment");

const MIN_QUERY_INTERVAL = 5000;
const MAX_QUERY_INTERVAL = 30000;

module.exports = class SmallMachineSource {

  constructor(workloadOpts) {
    this.workloadOpts = workloadOpts;

    this.sampleTypes = {
      counters: "TIME_COMPLEX",
      setup: "TIME_COMPLEX",
      geo: "TIME_COMPLEX",
      alarm: "INTERVAL",
    };

    this.sampleIntervals = {
      counters: 21600000,
      setup: 86400000,
      geo: 86400000,
      alarm: 604800000
    };

    this.sampleMethods = {
      counters: this.countersSample.bind(this),
      setup: this.setupSample.bind(this),
      geo: this.geoSample.bind(this),
      alarm: this.alarmSample.bind(this)
    };

    this.queryIntervals = {
      lastMonthProcessedQuantity: Math.min(Math.max(Math.floor(28800000 / this.workloadOpts.machines.length), MIN_QUERY_INTERVAL), MAX_QUERY_INTERVAL),
      lastTwoMonthsAlarms: Math.min(Math.max(Math.floor(28800000 / this.workloadOpts.machines.length), MIN_QUERY_INTERVAL), MAX_QUERY_INTERVAL),
      lastYearAggrProcessedQuantity: Math.min(Math.max(Math.floor(864000000 / this.workloadOpts.machines.length), MIN_QUERY_INTERVAL), MAX_QUERY_INTERVAL),
      lastYearCountersDifference: Math.min(Math.max(Math.floor(604800000 / this.workloadOpts.machines.length), MIN_QUERY_INTERVAL), MAX_QUERY_INTERVAL),
      oldSetup: Math.min(Math.max(Math.floor(864000000 / this.workloadOpts.machines.length), MIN_QUERY_INTERVAL), MAX_QUERY_INTERVAL)
    };

    this.queryPhases = {
      lastMonthProcessedQuantity: Math.floor(Math.random() * MAX_QUERY_INTERVAL),
      lastTwoMonthsAlarms: Math.floor(Math.random() * MAX_QUERY_INTERVAL),
      lastYearAggrProcessedQuantity: Math.floor(Math.random() * MAX_QUERY_INTERVAL),
      lastYearCountersDifference: Math.floor(Math.random() * MAX_QUERY_INTERVAL),
      oldSetup: Math.floor(Math.random() * MAX_QUERY_INTERVAL)
    };

    this.queryMethods = {
      lastMonthProcessedQuantity: this.lastMonthProcessedQuantity.bind(this),
      lastTwoMonthsAlarms: this.lastTwoMonthsAlarms.bind(this),
      lastYearAggrProcessedQuantity: this.lastYearAggrProcessedQuantity.bind(this),
      lastYearCountersDifference: this.lastYearCountersDifference.bind(this),
      oldSetup: this.oldSetup.bind(this)
    };
  }

  sample(id, groupName, absDate) {
    return {
      type: this.sampleTypes[groupName],
      value: this.sampleMethods[groupName](id, absDate)
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
          processedQuantity: Math.round(Math.random() * 1000000),
          workingTime: Math.round(Math.random() * 1000000)
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
          reservoirSize: Math.round(Math.random() * 50),
          interventionLevel: Math.round(Math.random() * 10),
          warningThreshold: Math.round(Math.random() * 10),
          alarmThreshold: Math.round(Math.random() * 5)
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

  lastMonthProcessedQuantity(absDate) {
    let machineIndex = Math.floor(this.workloadOpts.machines.length * this.workloadOpts.machineUptime * Math.random());

    return {
      name: "LAST_MONTH_PROCESSED_QUANTITY",
      type: "TIME_COMPLEX_RANGE",
      options: {
        deviceType: this.workloadOpts.machineTypeId,
        group: "counters",
        select: [ "processedQuantity" ],
        device: this.workloadOpts.machines[machineIndex],
        startTime: new Date(absDate.getTime() - 2592000000),
        endTime: absDate
      },
      interval: this.sampleIntervals.counters
    };
  }

  lastTwoMonthsAlarms(absDate) {
    let machineIndex = Math.floor(this.workloadOpts.machines.length * this.workloadOpts.machineUptime * Math.random());

    return {
      name: "LAST_TWO_MONTHS_ALARMS",
      type: "INTERVAL_RANGE",
      options: {
        deviceType: this.workloadOpts.machineTypeId,
        group: "alarm",
        device: this.workloadOpts.machines[machineIndex],
        startTime: new Date(absDate.getTime() - 5184000000),
        endTime: absDate
      },
      interval: this.sampleIntervals.alarm
    };
  }

  lastYearAggrProcessedQuantity(absDate) {
    let machineIndex = Math.floor(this.workloadOpts.machines.length * this.workloadOpts.machineUptime * Math.random());

    return {
      name: "LAST_YEAR_PROCESSED_QUANTITY",
      type: "TIME_COMPLEX_RANGE_BUCKET_AVG",
      options: {
        deviceType: this.workloadOpts.machineTypeId,
        group: "counters",
        select: [ "processedQuantity" ],
        device: this.workloadOpts.machines[machineIndex],
        startTime: new Date(absDate.getTime() - 31536000000),
        endTime: absDate,
        buckets: 1024
      },
      interval: this.sampleIntervals.counters
    };
  }

  lastYearCountersDifference(absDate) {
    let machineIndex = Math.floor(this.workloadOpts.machines.length * this.workloadOpts.machineUptime * Math.random());

    return {
      name: "LAST_YEAR_COUNTERS_DIFFERENCE",
      type: "TIME_COMPLEX_DIFFERENCE",
      options: {
        deviceType: this.workloadOpts.machineTypeId,
        group: "counters",
        device: this.workloadOpts.machines[machineIndex],
        startTime: new Date(absDate.getTime() - 31536000000),
        endTime: absDate,
      },
      interval: this.sampleIntervals.counters
    };
  }

  oldSetup(absDate) {
    let machineIndex = Math.floor(this.workloadOpts.machines.length * this.workloadOpts.machineUptime * Math.random());
    let yearTime = moment(absDate).startOf("year").valueOf();
    let nowTime = absDate.getTime();

    return {
      name: "LAST_MONTH_MACHINE_ENERGY",
      type: "TIME_COMPLEX_LAST_BEFORE",
      options: {
        deviceType: this.workloadOpts.machineTypeId,
        group: "counters",
        device: this.workloadOpts.machines[machineIndex],
        time: new Date(yearTime + (nowTime - yearTime) * Math.random())
      },
      interval: this.sampleIntervals.counters
    };
  }

};
