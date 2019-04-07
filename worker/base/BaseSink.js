"use strict";

module.exports = class BaseSink {

  constructor(databaseOpts, queryConcurrency, recordConcurrency) {
    this.databaseOpts = databaseOpts;

    this.queryConcurrency = parseInt(process.env.QUERY_CONCURRENCY) || queryConcurrency || 16;
    this.recordConcurrency = parseInt(process.env.RECORD_CONCURRENCY) || recordConcurrency || 4;

    this.latencyByType = {
      INTERVAL_RANGE: 0,
      TIME_COMPLEX_RANGE: 0,
      TIME_COMPLEX_RANGE_BUCKET_AVG: 0,
      TIME_COMPLEX_DIFFERENCE: 0,
      TIME_COMPLEX_LAST_BEFORE: 0
    };

    this.countByType = {
      INTERVAL_RANGE: 0,
      TIME_COMPLEX_RANGE: 0,
      TIME_COMPLEX_RANGE_BUCKET_AVG: 0,
      TIME_COMPLEX_DIFFERENCE: 0,
      TIME_COMPLEX_LAST_BEFORE: 0
    };

    this.readRowsByType = {
      INTERVAL_RANGE: 0,
      TIME_COMPLEX_RANGE: 0,
      TIME_COMPLEX_RANGE_BUCKET_AVG: 0,
      TIME_COMPLEX_DIFFERENCE: 0,
      TIME_COMPLEX_LAST_BEFORE: 0
    };
  }

  async init() { }

  async cleanup() {
    for (let k in this.latencyByType) {
      if (!this.countByType[k])
        continue;

      let latency = this.latencyByType[k]/this.countByType[k];

      let d = Math.pow(10, 2);
      latency = Math.round(latency * d) / d;

      console.log(`${k} avg. latency: ${latency}, tot. latency: ${this.latencyByType[k]}, count: ${this.countByType[k]}, read rows: ${this.readRowsByType[k]}`);
    }
  }

  async train() { }

  querySink(sourceStream) {
    let depth = 0;
    let end = false;

    let result = {
      reads: 0,
      successfulReads: 0,
      readRows: 0,
      totalReadLatency: 0,
      writes: 0,
      successfulWrites: 0,
      totalWriteLatency: 0,
      errors: 0
    };

    result.promise = new Promise((resolve) => {
      sourceStream.on("data", async (chunk) => {
        ++depth;
  
        if (depth === this.queryConcurrency)
          sourceStream.pause();
  
        try {
          let t0 = Date.now();
          let count = await this.query(chunk.name, chunk.type, chunk.options, chunk.interval);
          ++result.successfulReads;
          result.totalReadLatency += Date.now() - t0;
          result.readRows += count;

          ++this.countByType[chunk.type];
          this.latencyByType[chunk.type] += Date.now() - t0;
          this.readRowsByType[chunk.type] += count;
        }
        catch(err) {
          console.error(err);
          ++result.errors;
        }
  
        if (depth === this.queryConcurrency)
          sourceStream.resume();  
  
        --depth;
        ++result.reads;
  
        if (depth == 0 && end)
          resolve();
      });

      sourceStream.on("end", () => {
        end = true;

        if (depth === 0)
          resolve();
      });
    });

    return result;
  }

  async query() {
    throw new Error("Base class doesn't implement query method");
  }

  recordSink(sourceStream) {
    let depth = 0;
    let end = false;

    let result = {
      reads: 0,
      successfulReads: 0,
      readRows: 0,
      totalReadLatency: 0,
      writes: 0,
      successfulWrites: 0,
      totalWriteLatency: 0,
      errors: 0
    };

    result.promise = new Promise((resolve) => {
      sourceStream.on("data", async (chunk) => {
        ++depth;
  
        if (depth === this.recordConcurrency)
          sourceStream.pause();
  
        try {
          let t0 = Date.now();
          await this.record(chunk.id, chunk.groupName, chunk.sample, chunk.interval);
          ++result.successfulWrites;
          result.totalWriteLatency += Date.now() - t0;
        }
        catch(err) {
          console.error(err);
          ++result.errors;
        }
  
        if (depth === this.recordConcurrency)
          sourceStream.resume();  
  
        --depth;
        ++result.writes;
  
        if (depth == 0 && end)
          resolve();
      });
  
      sourceStream.on("end", () => {
        end = true;

        if (depth === 0)
          resolve();
      });
    });

    return result;
  }

  async record() {
    throw new Error("Base class doesn't implement record method");
  }

};
