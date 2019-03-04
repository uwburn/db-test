"use strict";

const { Writable } = require("stream");

const SINK_STATS_INTERVAL = 60000;

module.exports = class BaseSink {

  constructor(databaseOpts, queryHighWaterMark, recordHighWaterMark) {
    this.databaseOpts = databaseOpts;

    this.queryHighWaterMark = parseInt(process.env.QUERY_HIGH_WATERMARK) || queryHighWaterMark || 16;
    this.recordHighWaterMark = parseInt(process.env.RECORD_HIGH_WATERMARK) || recordHighWaterMark || 16;

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
  }

  async init() {
    this.sinkStatsInterval = setInterval(() => {
      for (let k in this.latencyByType) {
        if (!this.countByType[k])
          continue;

        let latency = this.latencyByType[k]/this.countByType[k];

        let d = Math.pow(10, 2);
        latency = Math.round(latency * d) / d;

        console.log(`${k} avg. latency: ${latency}, tot. latency: ${this.latencyByType[k]}, count: ${this.countByType[k]}`);
      }
    }, SINK_STATS_INTERVAL);
  }

  async cleanup() {
    clearInterval(this.sinkStatsInterval);
  }

  queryStream() {
    let result = {
      stream: Writable({
        objectMode: true,
        highWaterMark: this.queryHighWaterMark
      }),
      reads: 0,
      successfulReads: 0,
      readRows: 0,
      totalReadLatency: 0,
      writes: 0,
      successfulWrites: 0,
      totalWriteLatency: 0,
      errors: 0
    };

    result.stream._write = (chunk, enc, callback) => {
      let t0 = Date.now();
      this.query(chunk.name, chunk.type, chunk.options, chunk.interval).then((count) => {
        ++result.successfulReads;
        result.totalReadLatency += Date.now() - t0;
        result.readRows += count;

        ++this.countByType[chunk.type];
        this.latencyByType[chunk.type] += Date.now() - t0;
      }).catch((err) => {
        console.error(err);
        ++result.errors;
      }).then(() => {
        ++result.reads;
      }).then(callback);
    };

    result.stream._writev = (chunks, callback) => {
      let t0 = Date.now();

      let promises = chunks.map((chunk) => {
        chunk = chunk.chunk;

        return this.query(chunk.name, chunk.type, chunk.options, chunk.interval).then((count) => {
          ++result.successfulReads;
          result.totalReadLatency += Date.now() - t0;
          result.readRows += count;

          ++this.countByType[chunk.type];
          this.latencyByType[chunk.type] += Date.now() - t0;
        }).catch((err) => {
          console.error(err);
          ++result.errors;
        }).then(() => {
          ++result.reads;
        });
      });

      Promise.all(promises).then(() => {
        callback();
      });
    };

    return result;
  }

  async query() {
    throw new Error("Base class doesn't implement query method");
  }

  recordStream() {
    let result = {
      stream: Writable({
        objectMode: true,
        highWaterMark: this.recordHighWaterMark
      }),
      reads: 0,
      successfulReads: 0,
      readRows: 0,
      totalReadLatency: 0,
      writes: 0,
      successfulWrites: 0,
      totalWriteLatency: 0,
      errors: 0
    };

    result.stream._write = (chunk, enc, callback) => {
      let t0 = Date.now();
      this.record(chunk.id, chunk.groupName, chunk.sample, chunk.interval).then(() => {
        ++result.successfulWrites;
        result.totalWriteLatency += Date.now() - t0;
      }).catch((err) => {
        console.error(err);
        ++result.errors;
      }).then(() => {
        ++result.writes;
      }).then(callback);
    };

    result.stream._writev = (chunks, callback) => {
      let t0 = Date.now();

      let promises = chunks.map((chunk) => {
        chunk = chunk.chunk;

        return this.record(chunk.id, chunk.groupName, chunk.sample, chunk.interval).then(() => {
          ++result.successfulWrites;
          result.totalWriteLatency += Date.now() - t0;
        }).catch((err) => {
          console.error(err);
          ++result.errors;
        }).then(() => {
          ++result.writes;
        });
      });

      Promise.all(promises).then(() => {
        callback();
      });
    };

    return result;
  }

  async record() {
    throw new Error("Base class doesn't implement record method");
  }

};
