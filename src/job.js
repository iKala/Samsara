/**
 * Module dependencies
 */
const moment = require('moment');

/**
 * Utilities
 */
const PubSub = require('../utils/pubsub');
const Logger = require('../utils/logger');

/**
 * Configurations
 */

class Job {
  constructor({ name, data = {} }, config = {}) {
    this.name = name;
    this.data = data;
    this.config = config;

    const { credentials, projectId } = config;

    if (!credentials) {
      throw new Error('`credentials` is required for setting up the Google Cloud Pub/Sub');
    }

    this.pubsub = new PubSub({ credentials, projectId });
    this.logger = new Logger({ debug: config.debug });
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

    this.logger.log(`The job created on the ${topicName}`, { data: this.data, dataBuffer });

    return topic.publish(dataBuffer);
  }
}

module.exports = Job;
