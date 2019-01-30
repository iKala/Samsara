/**
 * Module dependencies
 */
const { Topic, Subscription } = require('@google-cloud/pubsub');

/**
 * Configurations
 */
const { topicSuffix, subscriptionName } = require('~config/pubsub');
const pubsub = require('./pubsub');

describe('pubsub (instance of ExtendedPubSub)', () => {
  jest.setTimeout(20000);

  const topicName = `test-${topicSuffix}`;
  afterEach(async () => {
    const [subscriptions] = await pubsub.getSubscriptions();
    if (subscriptions.find(subscription => subscription.name.endsWith(subscriptionName))) {
      await pubsub.subscription(subscriptionName).delete();
    }
    const [topics] = await pubsub.getTopics();
    if (topics.find(topic => topic.name.endsWith(topicName))) {
      await pubsub.topic(topicName).delete();
    }
  });

  describe('.createTopicIfNotExists', () => {
    test('the topic should be create when not exists', async () => {
      await pubsub.createTopicIfNotExists(topicName);

      const [topics] = await pubsub.getTopics();
      // Ensure the topic created.
      expect(topics.find(topic => topic.name.endsWith(topicName))).toEqual(expect.anything());
    });

    test('do nothing when the topic exists', async () => {
      let topics = [];

      await pubsub.createTopic(topicName);
      [topics] = await pubsub.getTopics();
      // Ensure the topic created.
      expect(topics.find(topic => topic.name.endsWith(topicName))).toEqual(expect.anything());

      await pubsub.createTopicIfNotExists(topicName);

      [topics] = await pubsub.getTopics();
      // Ensure the topic created.
      expect(topics.find(topic => topic.name.endsWith(topicName))).toEqual(expect.anything());
    });
  });

  describe('.createOrGetTopic', () => {
    test('it should return the topic even it is not exists', async () => {
      const topic = await pubsub.createOrGetTopic(topicName);
      expect(topic).toBeInstanceOf(Topic);
    });

    test('it should return the topic', async () => {
      await pubsub.createTopic(topicName);

      const topic = await pubsub.createOrGetTopic(topicName);

      expect(topic).toBeInstanceOf(Topic);
      expect(pubsub.topic(topicName).name).toBe(topic.name);
    });
  });

  describe('.createSubscriptionIfNotExists', () => {
    test('the subscription should be created with the topic instance', async () => {
      const topic = await pubsub.createOrGetTopic(topicName);
      await pubsub.createSubscriptionIfNotExists(topic, subscriptionName);

      const [subscriptions] = await topic.getSubscriptions();

      // Ensure the subscription created.
      expect(
        subscriptions.find(subscription => subscription.name.endsWith(subscriptionName)),
      ).toEqual(expect.anything());
    });
    test('the subscription should be created with the topic name', async () => {
      const topic = await pubsub.createOrGetTopic(topicName);
      await pubsub.createSubscriptionIfNotExists(topicName, subscriptionName);

      const [subscriptions] = await topic.getSubscriptions();

      // Ensure the subscription created.
      expect(
        subscriptions.find(subscription => subscription.name.endsWith(subscriptionName)),
      ).toEqual(expect.anything());
    });
    test('the subscription should be created even the topic is not exists', async () => {
      await pubsub.createSubscriptionIfNotExists(topicName, subscriptionName);

      const [subscriptions] = await pubsub.topic(topicName).getSubscriptions();

      // Ensure the subscription created.
      expect(
        subscriptions.find(subscription => subscription.name.endsWith(subscriptionName)),
      ).toEqual(expect.anything());
    });
  });

  describe('.createOrGetSubscription', () => {
    test('it should return the subscription even it is not exists', async () => {
      const subscription = await pubsub.createOrGetSubscription(topicName, subscriptionName);
      expect(subscription).toBeInstanceOf(Subscription);
    });

    test('it shoule return the subscrition', async () => {
      await pubsub.createTopic(topicName);
      await pubsub.topic(topicName).createSubscription(subscriptionName);

      const subscription = await pubsub.createOrGetSubscription(topicName, subscriptionName);
      expect(subscription).toBeInstanceOf(Subscription);
      expect(pubsub.topic(topicName).subscription(subscriptionName).name).toBe(subscription.name);
    });
  });
});
