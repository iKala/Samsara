/* eslint-disable no-await-in-loop */
/* eslint-disable no-underscore-dangle */
/**
 * Module dependencies
 */
const _ = require('lodash');
const { EventEmitter } = require('events');
const { v1 } = require('@google-cloud/pubsub');
const grpc = require('grpc');

/**
 * Utilities
 */
const Logger = require('../utils/logger');
const PubSub = require('../utils/pubsub');

const defaultMaxRetries = 200;

const cachedPubsubClient = {};

class Worker extends EventEmitter {
  constructor(config = {}) {
    super();

    const { credentials, projectId, topicSuffix } = config;

    if (!credentials) {
      throw new Error('`credentials` is required for setting up the Google Cloud Pub/Sub');
    }

    if (!cachedPubsubClient[projectId]) {
      cachedPubsubClient[projectId] = new PubSub({ credentials, projectId, grpc });
    }

    this.topicName = `${this.name}-${topicSuffix}`;
    this.pubsub = cachedPubsubClient[projectId];

    const configWithDefaultValue = _.defaults(config, { maxRetries: defaultMaxRetries });

    this.config = configWithDefaultValue;
    this.logger = new Logger({ debug: configWithDefaultValue.debug });
    this.subscriber = new v1.SubscriberClient({ credentials, projectId, grpc });
  }

  async bulk(
    jobName,
    callback = (jobDatas = [], done = () => { }, failed = () => { }) => { },
    options,
  ) {
    const bulkSize = options.bulkSize || 50;
    let inProgress = 0;
    const maxInProgress = Number(options.flowControl.maxMessages);

    const { topicSuffix } = this.config;

    if (!this.Subscription) {
      this.Subscription = await this.pubsub.createSubscriptionIfNotExists(
        `${jobName}-${topicSuffix}`,
        `${jobName}-${this.config.subscriptionName}`,
      );
    }

    const formattedSubscription = this.subscriber.subscriptionPath(
      this.config.projectId,
      `${jobName}-${this.config.subscriptionName}`,
    );


    // The maximum number of messages returned for this request.
    // Pub/Sub may return fewer than the number specified.
    const request = {
      subscription: formattedSubscription,
      maxMessages: bulkSize,
    };

    const timer = setInterval(async () => {
      if (inProgress < maxInProgress) {
        try {
          inProgress += 1;

          let isDoneOrFailed = false;

          // The subscriber pulls a specified number of messages.
          const [response] = await this.subscriber.pull(request);

          const data = response.receivedMessages.map(({ message }) => ({
            ...message.attributes,
            ...JSON.parse(message.data.toString()),
            jobId: message.messageId,
          }));
          const ackIds = response.receivedMessages.map(({ ackId }) => ackId);
          const jobIds = data.map(({ jobId }) => jobId);

          this.logger.log(`The job of ${jobName} is finished and submit the ack request`, jobIds);

          const doneCallback = async () => {
            if (isDoneOrFailed) {
              return;
            }

            this.logger.log(`[bulk] The job of ${jobName} is finished and submit the ack request`, jobIds);

            let latestError;
            let retry = -1;
            const maxRetries = this.config.maxRetries || defaultMaxRetries;

            do {
              try {
                const ackRequest = {
                  subscription: formattedSubscription,
                  ackIds,
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
            if (isDoneOrFailed) {
              return;
            }

            isDoneOrFailed = true;

            this.logger.log(`[bulk] The job of ${jobName} failed and submit the nack request, retry the message again`, jobIds);

            // Same to the comment of `doneCallback`.
            // There is no way to know when will the nack job done.
            let latestError;
            let retry = -1;
            const maxRetries = this.config.maxRetries || defaultMaxRetries;

            do {
              try {
                const modifyAckRequest = {
                  subscription: formattedSubscription,
                  ackIds,
                  // If this parameter is 0, a default value of 10 seconds is used.
                  ackDeadlineSeconds: 0,
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

          // Process the messages.
          await callback(
            data,
            doneCallback,
            failedCallback,
          );
        } catch (error) {
          // Failed to pull message.
          inProgress -= 1;
        }
      }
    });

    process.on('SIGTERM', () => {
      clearInterval(timer);
    });
  }

  async process(
    jobName,
    // eslint-disable-next-line no-unused-vars
    callback = (jobData = {}, done = () => { }, failed = () => { }) => { },
    options,
  ) {
    const { topicSuffix } = this.config;

    let inProgress = 0;
    const maxInProgress = Number(options.flowControl.maxMessages);
    // createSubscriptionIfNotExists
    if (!this.Subscription) {
      this.Subscription = await this.pubsub.createSubscriptionIfNotExists(
        `${jobName}-${topicSuffix}`,
        `${jobName}-${this.config.subscriptionName}`,
        options,
      );
    }
    const formattedSubscription = this.subscriber.subscriptionPath(
      this.config.projectId,
      `${jobName}-${this.config.subscriptionName}`,
    );

    // The maximum number of messages returned for this request.
    // Pub/Sub may return fewer than the number specified.
    const request = {
      subscription: formattedSubscription,
      maxMessages: 1,
    };

    const timer = setInterval(async () => {
      if (inProgress < maxInProgress) {
        try {
          inProgress += 1;

          // The subscriber pulls a specified number of messages.
          const [response] = await this.subscriber.pull(request);

          if (response.receivedMessages.length === 0) {
            inProgress -= 1;
            return;
          }

          // Process the messages.
          await Promise.all(
            response.receivedMessages.map(async ({ ackId, message }) => {
              const data = JSON.parse(message.data.toString());

              let isDoneOrFailed = false;

              const doneCallback = async () => {
                if (isDoneOrFailed) {
                  return;
                }

                isDoneOrFailed = true;

                this.logger.log(`The job of ${jobName} is finished and submit the ack request`, { message });

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
                if (isDoneOrFailed) {
                  return;
                }

                isDoneOrFailed = true;

                this.logger.log(`The job of ${jobName} failed and submit the nack request, retry the message again`, { message });

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
                      ackDeadlineSeconds: 0,
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

              await callback(
                { ...message.attributes, ...data, jobId: message.messageId },
                doneCallback,
                failedCallback,
              );
            }),
          );
        } catch (error) {
          // Failed to pull message.
          inProgress -= 1;
        }
      }
    });

    process.on('SIGTERM', () => {
      clearInterval(timer);
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
