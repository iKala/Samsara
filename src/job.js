/* eslint-disable no-await-in-loop */
/**
 * Module dependencies
 */
const _ = require('lodash');
const moment = require('moment');
const grpc = require('grpc');

/**
 * Utilities
 */
const PubSub = require('../utils/pubsub');
const Logger = require('../utils/logger');

/**
 * Configurations
 */

const defaultMaxRetries = 200;

class Job {
  constructor({ name, data = {} }, config = {}) {
    this.name = name;
    this.data = data;

    const configWithDefaultValue = _.defaults(config, { maxRetries: defaultMaxRetries });

    this.config = configWithDefaultValue;

    const { credentials, projectId } = configWithDefaultValue;

    if (!credentials) {
      throw new Error('`credentials` is required for setting up the Google Cloud Pub/Sub');
    }

    this.pubsub = new PubSub({ credentials, projectId, grpc });
    this.logger = new Logger({ debug: configWithDefaultValue.debug });
  }

  async save() {
    const { topicSuffix, batching = {} } = this.config;
    const topicName = `${this.name}-${topicSuffix}`;
    const topic = await this.pubsub.createOrGetTopic(topicName, { batching });

    const dataBuffer = Buffer.from(
      JSON.stringify({
        ...this.data,
        topicName,
        createdAt: moment().utc(),
      }),
    );

    let latestError;
    let retry = -1;
    const maxRetries = this.config.maxRetries || defaultMaxRetries;

    do {
      try {
        const message = await topic.publish(dataBuffer);
        this.logger.log(`The job created on the ${topicName}`, { data: this.data, dataBuffer });
        return message;
      } catch (error) {
        latestError = error;
        retry += 1;
        this.logger.log(`🔄 Retry ${retry}/${maxRetries}`);
      }

      if (retry > maxRetries) {
        retry = -1;
        this.logger.log('❌ Retry too much time.');
      }
    } while (retry !== -1);

    throw latestError;
  }
}

module.exports = Job;
