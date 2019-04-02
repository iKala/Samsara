/**
 * Module dependencies
 */
const moment = require('moment');

/**
 * Utilities
 */
const logger = require('~utils/logger');
const pubsub = require('~utils/pubsub');

/**
 * Configurations
 */
const { topicSuffix } = require('~config/pubsub');

class Job {
  constructor({ name, data = {} }, pubsubClient = pubsub) {
    this.name = name;
    this.data = data;

    this.pubsub = pubsubClient;
  }

  async save() {
    const topicName = `${this.name}-${topicSuffix}`;
    const topic = await this.pubsub.createOrGetTopic(topicName);

    const dataBuffer = Buffer.from(
      JSON.stringify({
        topicName,
        createdAt: moment().utc(),
      }),
    );

    logger.error(`The job created on the ${topicName}`, { data: this.data, dataBuffer });

    return topic.publisher().publish(dataBuffer, this.data);
  }
}

const createJob = (name, data) => new Job({ name, data });

module.exports = { default: createJob, Job };
