/* eslint-disable no-await-in-loop */
/**
 * Module dependencies
 */
const _ = require('lodash');
const moment = require('moment');
const { v1 } = require('@google-cloud/pubsub');
const grpc = require('grpc');

/**
 * Utilities
 */
const Logger = require('../utils/logger');

/**
 * Configurations
 */

class Job {
  constructor({ name, data = {} }, config = {}) {
    this.name = name;
    this.data = data;

    const configWithDefaultValue = _.defaults(config);

    this.config = configWithDefaultValue;

    const { credentials, projectId } = configWithDefaultValue;

    if (!credentials) {
      throw new Error('`credentials` is required for setting up the Google Cloud Pub/Sub');
    }

    this.logger = new Logger({ debug: configWithDefaultValue.debug });

    this.publisher = new v1.PublisherClient({ credentials, projectId, grpc });
  }

  async save() {
    const { projectId, topicSuffix, batching = {} } = this.config;
    const topicName = `${this.name}-${topicSuffix}`;

    const formattedTopic = this.publisher.topicPath(
      projectId,
      topicName,
    );

    const dataBuffer = Buffer.from(
      JSON.stringify({
        ...this.data,
        topicName,
        createdAt: moment().utc(),
      }),
    );

    const messagesElement = {
      data: dataBuffer,
    };
    const messages = [messagesElement];

    // Build the request
    const request = {
      topic: formattedTopic,
      messages: messages,
    };

    const retrySettings = {
      retryCodes: [
        10, // 'ABORTED'
        1, // 'CANCELLED',
        4, // 'DEADLINE_EXCEEDED'
        13, // 'INTERNAL'
        8, // 'RESOURCE_EXHAUSTED'
        14, // 'UNAVAILABLE'
        2, // 'UNKNOWN'
      ],
      backoffSettings: {
        initialRetryDelayMillis: 100,
        retryDelayMultiplier: 1.3,
        maxRetryDelayMillis: 60000,
        initialRpcTimeoutMillis: 5000,
        rpcTimeoutMultiplier: 1.0,
        maxRpcTimeoutMillis: 600000,
        totalTimeoutMillis: 600000,
      },
    };

    return await this.publisher.publish(request, {
      retry: retrySettings,
      batching,
    });
  }
}

module.exports = Job;
