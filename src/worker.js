/* eslint-disable no-await-in-loop */
/* eslint-disable no-underscore-dangle */
/**
 * Module dependencies
 */
const _  = require('lodash');
const { EventEmitter } = require('events');
const { v1 } = require('@google-cloud/pubsub');
const grpc = require('grpc');

/**
 * Utilities
 */
const Logger = require('../utils/logger');

const defaultMaxRetries = 200;

class Worker extends EventEmitter {
  constructor(config = {}) {
    super();

    const { credentials, projectId } = config;

    if (!credentials) {
      throw new Error('`credentials` is required for setting up the Google Cloud Pub/Sub');
    }

    const configWithDefaultValue = _.defaults(config, { maxRetries: defaultMaxRetries });

    this.config = configWithDefaultValue;
    this.logger = new Logger({ debug: configWithDefaultValue.debug });

    this.subscriber = new v1.SubscriberClient({ credentials, projectId, grpc });
  }

  async process(
    jobName,
    // eslint-disable-next-line no-unused-vars
    callback = (jobData = {}, done = () => { }, failed = () => { }) => { },
    options,
  ) {
    const { topicSuffix } = this.config;
    // Create a job queue name(topic) with topic suffix for preventing naming conflic.
    const topicName = `${jobName}-${topicSuffix}`;

    let inProgress = 0;
    const maxInProgress = Number(options.flowControl.maxMessages);

    const formattedSubscription = this.subscriber.subscriptionPath(
      this.config.projectId,
      `${topicName}-${this.config.subscriptionName}`,
    );

    // The maximum number of messages returned for this request.
    // Pub/Sub may return fewer than the number specified.
    const request = {
      subscription: formattedSubscription,
      maxMessages: 1,
    };

    setInterval(async () => {
      if (inProgress < maxInProgress) {
        try {
          inProgress += 1;

          // The subscriber pulls a specified number of messages.
          const [response] = await this.subscriber.pull(request);
          // Process the messages.
          response.receivedMessages.forEach(({ ackId, message }) => {
            const data = JSON.parse(message.data.toString());


            const doneCallback = async () => {
              this.logger.log(`The job of ${topicName} is finished and submit the ack request`, { message });

              let latestError;
              let retry = -1;
              const maxRetries = this.config.maxRetries || defaultMaxRetries;

              do {
                try {
                  const ackRequest = {
                    subscription: formattedSubscription,
                    ackIds: [ackId],
                  };

                  await this.subscriber.acknowledge(ackRequest);
                  inProgress = inProgress > 0 ? inProgress - 1 : 0;

                  return;
                } catch (error) {
                  latestError = error;
                  retry += 1;
                  this.logger.log(`🔄 Retry to ack message ${retry}/${maxRetries}`);
                }

                if (retry > maxRetries) {
                  retry = -1;
                  this.logger.log('❌ Retry too much time.');
                }
              } while (retry !== -1);

              throw latestError;
            };

            const failedCallback = async () => {
              this.logger.log(`The job of ${topicName} failed and submit the nack request, retry the message again`, { message });

              // Same to the comment of `doneCallback`.
              // There is no way to know when will the nack job done.
              let latestError;
              let retry = -1;
              const maxRetries = this.config.maxRetries || defaultMaxRetries;

              do {
                try {
                  const modifyAckRequest = {
                    subscription: formattedSubscription,
                    ackIds: [ackId],
                    // If this parameter is 0, a default value of 10 seconds is used.
                    ackDeadlineSeconds: 10,
                  };

                  // If the message is not yet processed, reset its ack deadline.
                  await this.subscriber.modifyAckDeadline(modifyAckRequest);

                  inProgress = inProgress > 0 ? inProgress - 1 : 0;

                  return;
                } catch (error) {
                  latestError = error;
                  retry += 1;
                  this.logger.log(`🔄 Retry to nack message ${retry}/${maxRetries}`);
                }

                if (retry > maxRetries) {
                  retry = -1;
                  this.logger.log('❌ Retry too much time.');
                }
              } while (retry !== -1);

              throw latestError;
            };

            callback(
              { ...message.attributes, ...data, jobId: message.id },
              doneCallback,
              failedCallback,
            );
          });
        } catch (error) {
          // Failed to pull message.
          inProgress -= 1;
        }
      }
    });
  }

  shutdown() {
    const subscriptions = Object.values(this.subscriptions);

    subscriptions.forEach((subscription) => {
      this.logger.log('Shutting down the subscription of worker', { subscription });
      subscription.removeListener('message', () => { });
    });

    // Flush all subscription caches.
    this.subscriptions = {};
  }
}

module.exports = Worker;
