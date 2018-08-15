"use strict";

module.exports = require(`./BulkMachineData`)(`Small`, {
    counters: `recordTimeComplex`,
    setup: `recordTimeComplex`,
    mng: `recordTimeComplex`,
    geo: `recordTimeComplex`,
    alarm: `recordInterval`,
});
