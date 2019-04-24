/**
 * Module dependencies
 */
const { EventEmitter } = require('events');
const moment = require('moment');

/**
 * Utilities
 */
const PubSub = require('../utils/pubsub');

class Worker extends EventEmitter {
  constructor(config = {}) {
    super();

    const { credentials, projectId } = config;

    if (!credentials) {
      throw new Error('`credentials` is required for setting up the Google Cloud Pub/Sub');
    }

    this.config = config;
    this.pubsub = new PubSub({ credentials, projectId });

    this.subscriptions = {};
  }

  async getSubscription(topicName, options) {
    const { subscriptionName } = this.config;

    // Establish the new subscription when it is not exists
    if (!this.subscriptions[topicName]) {
      // Combine topic name with subscription name to allow a worker building multi subscriptions.
      this.subscriptions[topicName] = await this.pubsub
        .createOrGetSubscription(topicName, `${topicName}-${subscriptionName}`, options);
    }

    return this.subscriptions[topicName];
  }

  async process(
    jobName,
    // eslint-disable-next-line no-unused-vars
    callback = (jobData = {}, done = () => { }) => { },
    options,
  ) {
    const { topicSuffix } = this.config;
    // Create a job queue name(topic) with topic suffix for preventing naming conflic.
    const topicName = `${jobName}-${topicSuffix}`;
    const subscription = await this.getSubscription(topicName, options);

    subscription.on('message', (message) => {
      const doneCallback = () => {
        console.log(`The job of ${topicName} is finished and submit the ack request`, { message });

        // Since the message ack not support promise for now (google/pubsub repo WIP).
        // We have no way to know the exactly time when the ack job done.
        message.ack();
      };
      callback({ ...message.attributes, jobId: message.id }, doneCallback);
    });
    subscription.on('error', (error) => {
      console.log(`The job of ${topicName} failed at ${moment().utc()}`, error);
      this.emit('error', error);
    });
  }

  shutdown() {
    const subscriptions = Object.values(this.subscriptions);

    subscriptions.forEach((subscription) => {
      console.log('Shutting down the subscription of worker', { subscription });
      subscription.removeListener('message', () => { });
    });

    // Flush all subscription caches.
    this.subscriptions = {};
  }
}

module.exports = Worker;
