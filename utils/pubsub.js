/**
 * Module dependencies
 */
const { PubSub, Topic } = require('@google-cloud/pubsub');
const _ = require('lodash');

class ExtendedPubSub extends PubSub {
  async createTopicIfNotExists(name) {
    const [topics] = await this.getTopics();

    if (topics.find(topic => topic.name.endsWith(name))) {
      return Promise.resolve();
    }

    return this.createTopic(name);
  }

  async createSubscriptionIfNotExists(topicOrName, name, options = {}) {
    const topic = await (() => {
      if (_.isString(topicOrName)) {
        return this.createOrGetTopic(topicOrName);
      }

      if (topicOrName instanceof Topic) {
        return topicOrName;
      }

      throw new Error('topicOrName must be string or topic instance.');
    })();

    const [subscriptions] = await topic.getSubscriptions();

    if (subscriptions.find(subscription => subscription.name.endsWith(name))) {
      return Promise.resolve();
    }

    return topic.createSubscription(name, options);
  }

  async createOrGetTopic(name) {
    await this.createTopicIfNotExists(name);

    return this.topic(name);
  }

  async createOrGetSubscription(topicOrName, name, options = {}) {
    await this.createSubscriptionIfNotExists(topicOrName, name, options);

    const topic = await (() => {
      if (_.isString(topicOrName)) {
        return this.topic(topicOrName);
      }

      if (topicOrName instanceof Topic) {
        return topicOrName;
      }

      throw new Error('topicOrName must be string or topic instance.');
    })();

    return topic.subscription(name);
  }
}

module.exports = ExtendedPubSub;
