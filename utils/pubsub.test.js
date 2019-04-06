/**
 * Module dependencies
 */
const { Topic, Subscription } = require('@google-cloud/pubsub');

/**
 * Configurations
 */
const { credentials, topicSuffix, subscriptionName } = require('../config/pubsub');
const PubSub = require('./pubsub');

const pubsub = new PubSub({ credentials });

describe('pubsub (instance of ExtendedPubSub)', () => {
  jest.setTimeout(200000);

  const resetAll = async () => {
    const [subscriptions] = await pubsub.getSubscriptions();
    const matchedSubscriptions = subscriptions
      .filter(subscription => subscription.name.endsWith(subscriptionName));

    await Promise.all(
      matchedSubscriptions
        .map(subscription => {
          return subscription.delete();
        }),
    )

    const [topics] = await pubsub.getTopics();
    const matchedTopics = topics
      .filter(topic => topic.name.endsWith(topicSuffix));

    await Promise.all(
      matchedTopics
        .map(topic => topic.delete()),
    )
  };

  const topicName = `test-${topicSuffix}`;
  beforeEach(resetAll);
  afterEach(resetAll);

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

    test('it shoule return the subscription', async () => {
      await pubsub.createTopic(topicName);
      await pubsub.topic(topicName).createSubscription(subscriptionName);

      const subscription = await pubsub.createOrGetSubscription(topicName, subscriptionName);
      expect(subscription).toBeInstanceOf(Subscription);
      expect(pubsub.topic(topicName).subscription(subscriptionName).name).toBe(subscription.name);
    });
  });
});
