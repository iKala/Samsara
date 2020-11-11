/**
 * Module dependencies
 */
const { PubSub, Topic } = require('@google-cloud/pubsub');
const _ = require('lodash');

class ExtendedPubSub extends PubSub {

  // Get topic by name
  async getTopicMatchName(topicName) {
    const [topics] = await this.getTopics();

    const foundTopic = topics.find(topic => topic.name.endsWith(topicName));
    // console.log(foundTopic);
    if (foundTopic) {
      return foundTopic;
    }
    return null;
  }

  async createSubscriptionIfNotExists(topicName, name, options = {}) {
    const topic = await (() => {
      if (_.isString(topicName)) {
        return this.createOrGetTopic(topicName);
      }

      throw new Error('topicName must be string.');
    })();
    const [subscriptions] = await topic.getSubscriptions();

    if (subscriptions.find(subscription => subscription.name.endsWith(name))) {
      return Promise.resolve();
    }

    return this.createSubscription(topicName, name, options);
  }

  /**
   * Get topic or create.
   * @param {string} name The name of the topic
   * @returns {Topic} 
   */
  async createOrGetTopic(name) {
    const topic = await this.getTopicMatchName(name);
    if (!topic) {
      const createdTopic = await this.createTopic(name);
      // https://googleapis.dev/nodejs/pubsub/1.7.1/PubSub.html#createTopic
      // [0]: The new topic.
      // [1]: The full API response.
      return createdTopic[0];
    }
    return topic;
  }

  async createOrGetSubscription(topicOrName, name, options = {}) {
    await this.createSubscriptionIfNotExists(topicOrName, name, options);

    return this.subscription(name, options);
  }
}

module.exports = ExtendedPubSub;
