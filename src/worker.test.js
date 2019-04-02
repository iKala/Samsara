/**
 * Module dependencies
 */
const proxyquire = require('proxyquire');
const sinon = require('sinon');
const googlePubsub = require('@google-cloud/pubsub');
const assert = require('assert');
const through2 = require('through2');
const moment = require('moment');

/**
 * Configurations
 */
const { topicSuffix, subscriptionName } = require('~config/pubsub');

const { Worker } = require('./worker');

const mockBidiStreamingGrpcMethod = (expectedRequest, response, error) => () => {
  const mockStream = through2.obj((chunk, enc, callback) => {
    assert.deepStrictEqual(chunk, expectedRequest);
    if (error) {
      callback(error);
    } else {
      callback(null, response);
    }
  });
  mockStream.cancel = () => { };
  return mockStream;
};

const client = new googlePubsub.v1.SubscriberClient({
  credentials: { client_email: 'bogus', private_key: 'bogus' },
  projectId: 'bogus',
});

// Mock projectify for all modules.
jest.mock('@google-cloud/projectify', () => ({
  replaceProjectIdToken: () => ({}),
}));

describe('queue/Worker', () => {
  jest.setTimeout(2000);

  describe('.process', () => {
    const sandbox = sinon.createSandbox();
    const testJobName = 'test';
    const topicName = `${testJobName}-${topicSuffix}`;
    const jobData = { payload: 'foo' };

    // Mock request
    const request = {
      subscription: {},
      streamAckDeadlineSeconds: 10,
    };

    const receivedMessages = [
      {
        message: {
          publishTime: moment(),
          data: '',
          attributes: jobData,
        },
      },
    ];

    let worker;

    beforeEach(() => {
      const pubsub = proxyquire(
        '~utils/pubsub',
        {
          '@google-cloud/pubsub': googlePubsub,
        },
      );

      sandbox
        // eslint-disable-next-line
        .stub(client._innerApiCalls, 'streamingPull')
        .value(mockBidiStreamingGrpcMethod(request, { receivedMessages }));

      sandbox
        .stub(pubsub, 'getClient_')
        .value((_, callback) => {
          // Mock Grpc layer
          callback(null, client);
        });
      sandbox.stub(pubsub, 'getTopics').returns([[{ name: `${topicName}` }]]);
      sandbox
        .stub(googlePubsub.Topic.prototype, 'getSubscriptions')
        .returns([[{ name: `${topicName}-${subscriptionName}` }]]);
      sandbox
        .stub(pubsub, 'request').value((config, callback) => {
          callback(null, {});
        });

      worker = new Worker({}, pubsub);
    });

    afterEach(() => {
      sandbox.restore();
    });

    test('should process job', (testDone) => {
      worker.process(testJobName, (_, jobDone) => {
        jobDone();
        testDone();
      });
    });

    test('should receive the data', (testDone) => {
      worker.process(testJobName, (data, jobDone) => {
        expect(data).toEqual(jobData);
        jobDone();
        testDone();
      });
    });
  });

  describe('.shutdown', () => {
    const sandbox = sinon.createSandbox();

    // Mock request
    const request = {
      subscription: {},
      streamAckDeadlineSeconds: 10,
    };

    let worker;
    beforeEach(() => {
      const pubsub = proxyquire(
        '~utils/pubsub',
        {
          '@google-cloud/pubsub': googlePubsub,
        },
      );

      sandbox
        // eslint-disable-next-line
        .stub(client._innerApiCalls, 'streamingPull')
        .value(mockBidiStreamingGrpcMethod(request, { receivedMessages: [] }));

      sandbox
        .stub(pubsub, 'getClient_')
        .value((_, callback) => {
          // Mock Grpc layer
          callback(null, client);
        });

      sandbox.stub(pubsub, 'getTopics').returns([[{ name: 'test-1' }, { name: 'test-2' }]]);
      sandbox
        .stub(googlePubsub.Topic.prototype, 'getSubscriptions')
        .returns([[{ name: `test-1-${subscriptionName}` }, { name: `test-1-${subscriptionName}` }]]);
      sandbox
        .stub(pubsub, 'request').value((config, callback) => {
          callback(null, {});
        });

      worker = new Worker({}, pubsub);
    });

    afterEach(() => {
      sandbox.restore();
    });

    test('should remove all subscriptions after shuting down', async () => {
      await worker.process('test-1', () => { });
      await worker.process('test-2', () => { });

      expect(Object.values(worker.subscriptions).length).toBe(2);

      worker.shutdown();

      expect(Object.values(worker.subscriptions).length).toBe(0);
    });
  });
});
