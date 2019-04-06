const Job = require('./job');
const Worker = require('./worker');

module.exports = (config) => ({
  createJob: (name, data) => new Job({ name, data }, config),
  createWorker: () => new Worker(config),
});
