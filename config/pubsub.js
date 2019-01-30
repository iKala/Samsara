require('dotenv').config();

/**
 * Module dependencies
 */
const fs = require('fs');
const randomize = require('randomatic');

// Automatic generate a topic suffix when the specific suffix not exists.
const hasPubsubTopicSuffixEnv = !!process.env.GOOGLE_PUBSUB_TOPIC_SUFFIX;
if (!hasPubsubTopicSuffixEnv) {
  const topicSuffix = `${randomize('Aa0', 10)}-${randomize('Aa0', 4)}`;
  fs.appendFileSync('.env', `\nGOOGLE_PUBSUB_TOPIC_SUFFIX=${topicSuffix}`);

  // Prepare the topic name suffix.
  process.env.GOOGLE_PUBSUB_TOPIC_SUFFIX = topicSuffix;
}

const hasPubsubSubscriptionNameEnv = !!process.env.GOOGLE_PUBSUB_SUBSCRIPTION_NAME;
if (!hasPubsubSubscriptionNameEnv) {
  const subcriptionName = `${randomize('Aa0', 10)}-${randomize('Aa0', 4)}`;
  fs.appendFileSync('.env', `\nGOOGLE_PUBSUB_SUBSCRIPTION_NAME=${subcriptionName}`);

  // Prepare the topic name suffix.
  process.env.GOOGLE_PUBSUB_SUBSCRIPTION_NAME = subcriptionName;
}

module.exports = {
  credentials: {
    client_email: process.env.GOOGLE_CREDENTIAL_CLIENT_EMAIL,
    private_key: process.env.GOOGLE_CREDENTIAL_PRIVATE_KEY,
  },
  topicSuffix: process.env.GOOGLE_PUBSUB_TOPIC_SUFFIX,
  subscriptionName: process.env.GOOGLE_PUBSUB_SUBSCRIPTION_NAME,
};
