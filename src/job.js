/**
 * Module dependencies
 */
const moment = require('moment');

/**
 * Utilities
 */
const PubSub = require('~utils/pubsub');

/**
 * Configurations
 */

class Job {
  constructor({ name, data = {} }, config = {}) {
    this.name = name;
    this.data = data;
    this.config = config;

    const { credentials } = config;

    if (!credentials) {
      throw new Error('`credentials` is required for setting up the Google Cloud Pub/Sub');
    }

    this.pubsub = new PubSub({ credentials });
  }

  async save() {
    const { topicSuffix } = this.config;
    const topicName = `${this.name}-${topicSuffix}`;
    const topic = await this.pubsub.createOrGetTopic(topicName);

    const dataBuffer = Buffer.from(
      JSON.stringify({
        topicName,
        createdAt: moment().utc(),
      }),
    );

    console.log(`The job created on the ${topicName}`, { data: this.data, dataBuffer });

    return topic.publisher().publish(dataBuffer, this.data);
  }
}

module.exports = { default: Job, Job };
