"use strict";

module.exports = require(`./BulkMachineData`)(`Big`, {
    status: `recordInterval`,
    counters: `recordTimeComplex`,
    setup: `recordTimeComplex`,
    temperatureProbe1: `recordTimeComplex`,
    temperatureProbe2: `recordTimeComplex`,
    alarm: `recordInterval`,
});
