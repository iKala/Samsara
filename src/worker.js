/**
 * Module dependencies
 */
const { EventEmitter } = require('events');
const moment = require('moment');

/**
 * Utilities
 */
const logger = require('~utils/logger');
const pubsub = require('~utils/pubsub');

/**
 * Configurations
 */
const { topicSuffix, subscriptionName } = require('~config/pubsub');

class Worker extends EventEmitter {
  constructor(config = {}, pubsubClient = pubsub) {
    super();

    this.config = config;
    this.pubsub = pubsubClient;
    this.subscriptions = {};
  }

  async getSubscription(topicName) {
    // Establish the new subscription when it is not exists
    if (!this.subscriptions[topicName]) {
      // Combine topic name with subscription name to allow a worker building multi subscriptions.
      this.subscriptions[topicName] = await this.pubsub
        .createOrGetSubscription(topicName, `${topicName}-${subscriptionName}`, this.config);
    }

    return this.subscriptions[topicName];
  }

  async process(
    jobName,
    // eslint-disable-next-line no-unused-vars
    callback = (jobData = {}, done = () => { }) => { },
  ) {
    // Create a job queue name(topic) with topic suffix for preventing naming conflic.
    const topicName = `${jobName}-${topicSuffix}`;
    const subscription = await this.getSubscription(topicName);

    subscription.on('message', (message) => {
      const doneCallback = () => {
        logger.log(`The job of ${topicName} is finished and submit the ack request`, { message });

        // Since the message ack not support promise for now (google/pubsub repo WIP).
        // We have no way to know the exactly time when the ack job done.
        message.ack();
      };
      callback(message.attributes, doneCallback);
    });
    subscription.on('error', (error) => {
      logger.error(`The job of ${topicName} failed at ${moment().utc()}`, error);
      this.emit('error', error);
    });
  }

  shutdown() {
    const subscriptions = Object.values(this.subscriptions);

    subscriptions.forEach((subscription) => {
      logger.log('Shutting down the subscription of worker', { subscription });
      subscription.removeListener('message', () => { });
    });

    // Flush all subscription caches.
    this.subscriptions = {};
  }
}

const worker = new Worker();

module.exports = { default: worker, Worker };
