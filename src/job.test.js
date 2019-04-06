/**
 * Module dependencies
 */
const proxyquire = require('proxyquire');
const sinon = require('sinon');
const googlePubsub = require('@google-cloud/pubsub');
const { Publisher } = require('@google-cloud/pubsub/build/src/publisher');

/**
 * Configurations
 */
const { topicSuffix } = require('../config/pubsub');

const { Job } = require('./job');

describe('queue/Job', () => {
  jest.setTimeout(2000);

  describe('.save', () => {
    const sandbox = sinon.createSandbox();
    const testJobName = 'test';
    const topicName = `${testJobName}-${topicSuffix}`;
    const jobData = { payload: 'foo' };

    let publisher;
    let job;

    beforeEach(() => {
      const PubSub = proxyquire(
        '../utils/pubsub',
        {
          '@google-cloud/pubsub': googlePubsub,
        },
      );

      const pubsub = new PubSub();

      publisher = new Publisher(topicName);

      sandbox.stub(pubsub, 'getTopics').returns([[{ name: `${topicName}` }]]);
      sandbox.stub(googlePubsub.Topic.prototype, 'publisher').returns(publisher);
      sandbox
        .stub(pubsub, 'request').value((config, callback) => {
          callback(null, {});
        });

      job = new Job({ name: testJobName, data: jobData }, { credentials: {} });
      job.pubsub = pubsub;
    });

    afterEach(() => {
      sandbox.restore();
    });
    test('should create job to queue', async (done) => {
      // eslint-disable-next-line
      publisher.queue_ = (data, attrs, callback) => {
        expect(attrs).toEqual(jobData);
        callback();
        done();
      };
      await job.save();
    });

    test('should get job id when created', async () => {
      // eslint-disable-next-line
      publisher.queue_ = (data, attrs, callback) => {
        callback();
      };
      const jobId = await job.save();
      expect(jobId).toEqual(expect.anything());
    });
  });
});
