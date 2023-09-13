const AWS = require('aws-sdk');
const { SQS } = require('@aws-sdk/client-sqs');
const { S3 } = require('@aws-sdk/client-s3');
const uuid = require('uuid/v4');

const ExtendedSqsClient = require('../src/ExtendedSqsClient');

const sqsEndpoint = process.env.SQS_ENDPOINT || 'http://localhost:4566';
const sqsV2 = new AWS.SQS({
    apiVersion: '2012-11-05',
    endpoint: sqsEndpoint,
    region: 'eu-west-2',
});
const sqsV3 = new SQS({
    apiVersion: '2012-11-05',
    endpoint: sqsEndpoint,
    region: 'eu-west-2',
});

const s3Endpoint = process.env.S3_ENDPOINT || 'http://localhost:4566';
const s3V2 = new AWS.S3({
    apiVersion: '2006-03-01',
    endpoint: s3Endpoint,
    region: 'eu-west-2',
    s3ForcePathStyle: true,
});
const s3V3 = new S3({
    apiVersion: '2006-03-01',
    endpoint: s3Endpoint,
    region: 'eu-west-2',
    forcePathStyle: true,
});

jest.setTimeout(60000);

let queueUrl;
const bucketName = uuid();

const largeMessageBody = 'large body'.repeat(1024 * 1024);
const largeMessageBody2 = 'large body2'.repeat(1024 * 1024);

beforeEach(async () => {
    queueUrl = (await sqsV2.createQueue({ QueueName: uuid() }).promise()).QueueUrl;
    await s3V2.createBucket({ Bucket: bucketName }).promise();
});

afterEach(async () => {
    await sqsV2.deleteQueue({ QueueUrl: queueUrl }).promise();
    await s3V2.deleteBucket({ Bucket: bucketName }).promise();
});

async function s3ObjectCount() {
  return (await s3V2.listObjectsV2({ Bucket: bucketName }).promise()).KeyCount;
}

describe.each([
  [sqsV2, s3V2],
  [sqsV3, s3V3],
])('sqs-extended-client - %#', (sqs, s3) => {

    it('should send/receive/delete small message not using S3', async () => {
        // Given
        const sqsExtendedClientSend = new ExtendedSqsClient(sqs, s3, { bucketName });
        const sqsExtendedClient = new ExtendedSqsClient(sqs, s3);

        // When
        const sendMessageResult = sqsExtendedClientSend.sendMessage({
            QueueUrl: queueUrl,
            MessageBody: 'small body',
        })
        'promise' in sendMessageResult ? await sendMessageResult.promise() : await sendMessageResult;

        const receiveMessageResult = sqsExtendedClient.receiveMessage({
            QueueUrl: queueUrl,
        })
        const { Messages: messages } = 'promise' in receiveMessageResult ? await receiveMessageResult.promise() : await receiveMessageResult;

        const s3Objects = await s3ObjectCount();

        const deleteMessageResult = sqsExtendedClient.deleteMessage({
            QueueUrl: queueUrl,
            ReceiptHandle: messages[0].ReceiptHandle,
        })
        'promise' in deleteMessageResult ? await deleteMessageResult.promise() : await deleteMessageResult;

        // Then
        expect(messages[0].Body).toBe('small body');

        expect(s3Objects).toBe(0);
        expect(await s3ObjectCount()).toBe(0);
    });

    it('should send/receive/delete large message using S3', async () => {
        // Given
        const sqsExtendedClientSend = new ExtendedSqsClient(sqs, s3, { bucketName });
        const sqsExtendedClient = new ExtendedSqsClient(sqs, s3);

        // When
        const sendMessageResult = sqsExtendedClientSend.sendMessage({
            QueueUrl: queueUrl,
            MessageBody: largeMessageBody,
        })
        'promise' in sendMessageResult ? await sendMessageResult.promise() : await sendMessageResult;

        const receiveMessageResult = sqsExtendedClient.receiveMessage({
            QueueUrl: queueUrl,
        })
        const { Messages: messages } = 'promise' in receiveMessageResult ? await receiveMessageResult.promise() : await receiveMessageResult;

        const s3Objects = await s3ObjectCount();

        const deleteMessageResult = sqsExtendedClient.deleteMessage({
            QueueUrl: queueUrl,
            ReceiptHandle: messages[0].ReceiptHandle,
        })
        'promise' in deleteMessageResult ? await deleteMessageResult.promise() : await deleteMessageResult;

        // Then
        expect(messages[0].Body).toBe(largeMessageBody);

        expect(s3Objects).toBe(1);
        expect(await s3ObjectCount()).toBe(0);
    });

    it('should send/receive/delete using custom transforms', async () => {
        // Given
        const sendTransform = (sqsMessage) => {
            const { largeItem, ...messageBody } = JSON.parse(sqsMessage.MessageBody);
            return {
                s3Content: largeItem,
                messageBody: JSON.stringify(messageBody),
            };
        };

        const receiveTransform = (sqsMessage, s3Content) =>
            JSON.stringify({
                ...JSON.parse(sqsMessage.Body),
                largeItem: s3Content,
            });

        const sqsExtendedClientSend = new ExtendedSqsClient(sqs, s3, { bucketName, sendTransform });
        const sqsExtendedClient = new ExtendedSqsClient(sqs, s3, { receiveTransform });

        const bodyObj = { smallItem: 'small', largeItem: 'large'.repeat(1024 * 1024) };

        // When
        const sendMessageResult = sqsExtendedClientSend.sendMessage({
            QueueUrl: queueUrl,
            MessageBody: JSON.stringify(bodyObj),
        })
        'promise' in sendMessageResult ? await sendMessageResult.promise() : await sendMessageResult;

        const receiveMessageResult = sqsExtendedClient.receiveMessage({
            QueueUrl: queueUrl,
        })
        const { Messages: messages } = 'promise' in receiveMessageResult ? await receiveMessageResult.promise() : await receiveMessageResult;

        const s3Objects = await s3ObjectCount();

        const deleteMessageResult = sqsExtendedClient.deleteMessage({
            QueueUrl: queueUrl,
            ReceiptHandle: messages[0].ReceiptHandle,
        })
        'promise' in deleteMessageResult ? await deleteMessageResult.promise() : await deleteMessageResult;

        // Then
        expect(JSON.parse(messages[0].Body)).toEqual(bodyObj);

        expect(s3Objects).toBe(1);
        expect(await s3ObjectCount()).toBe(0);
    });

    it('should send/receive/delete batch using S3', async () => {
        // Given
        const sqsExtendedClientSend = new ExtendedSqsClient(sqs, s3, { bucketName });
        const sqsExtendedClient = new ExtendedSqsClient(sqs, s3);

        // When
        const sendMessageResult = sqsExtendedClientSend.sendMessageBatch({
            QueueUrl: queueUrl,
            Entries: [
                {
                    Id: '1',
                    MessageBody: largeMessageBody,
                },
                {
                    Id: '2',
                    MessageBody: 'small body',
                },
                {
                    Id: '3',
                    MessageBody: largeMessageBody2,
                },
            ],
        })
        'promise' in sendMessageResult ? await sendMessageResult.promise() : await sendMessageResult;

        // SQS does not guarantee receiving up to `MaxNumberOfMessages` messages from a queue in a single request because of it's distributed computing architecture,
        // so we need to request a few times to get all the messages, especially when there are a low number of messages in the queue
        const messages = []
        let attempts = 10
        while (attempts > 0 && messages.length < 3) {
            const receiveMessageResult = sqsExtendedClient.receiveMessage({ QueueUrl: queueUrl, MaxNumberOfMessages: 10 })
            const { Messages } = 'promise' in receiveMessageResult ? await receiveMessageResult.promise() : await receiveMessageResult;
            if (Messages) {
                messages.push(...Messages)
            }
      
            await new Promise((resolve) => setTimeout(resolve, 10))
            attempts -= 1
        }

        const s3Objects = await s3ObjectCount();

        const deleteMessageResult = sqsExtendedClient.deleteMessageBatch({
            QueueUrl: queueUrl,
            Entries: [
                {
                    Id: '1',
                    ReceiptHandle: messages[0].ReceiptHandle,
                },
                {
                    Id: '2',
                    ReceiptHandle: messages[1].ReceiptHandle,
                },
                {
                    Id: '3',
                    ReceiptHandle: messages[2].ReceiptHandle,
                },
            ],
        })
        'promise' in deleteMessageResult ? await deleteMessageResult.promise() : await deleteMessageResult;

        // Then
        messages.sort((m1, m2) => m1.Body.localeCompare(m2.Body));

        expect(messages[0].Body).toBe(largeMessageBody2);
        expect(messages[1].Body).toBe(largeMessageBody);
        expect(messages[2].Body).toBe('small body');

        expect(s3Objects).toBe(2);
        expect(await s3ObjectCount()).toBe(0);
    });

    it('should change message visibility', async () => {
        // Given
        const sqsExtendedClientSend = new ExtendedSqsClient(sqs, s3, { bucketName });
        const sqsExtendedClient = new ExtendedSqsClient(sqs, s3);

        // When
        const sendMessageResult = sqsExtendedClientSend.sendMessage({
            QueueUrl: queueUrl,
            MessageBody: largeMessageBody,
        })
        'promise' in sendMessageResult ? await sendMessageResult.promise() : await sendMessageResult;

        const receiveMessageResult1 = sqsExtendedClient.receiveMessage({
            QueueUrl: queueUrl,
        })
        const { Messages: messages1 } = 'promise' in receiveMessageResult1 ? await receiveMessageResult1.promise() : await receiveMessageResult1;

        // expect message to be invisible
        const receiveMessageResult2 = sqsExtendedClient.receiveMessage({
            QueueUrl: queueUrl,
        })
        const { Messages: messages2 } = 'promise' in receiveMessageResult2 ? await receiveMessageResult2.promise() : await receiveMessageResult2;

        const changeVisibilityResult = sqsExtendedClient.changeMessageVisibility({
            QueueUrl: queueUrl,
            ReceiptHandle: messages1[0].ReceiptHandle,
            VisibilityTimeout: 0,
        })
        'promise' in changeVisibilityResult ? await changeVisibilityResult.promise() : await changeVisibilityResult;

        // expect message to be invisible
        const receiveMessageResult3 = sqsExtendedClient.receiveMessage({
            QueueUrl: queueUrl,
        })
        const { Messages: messages3 } = 'promise' in receiveMessageResult3 ? await receiveMessageResult3.promise() : await receiveMessageResult3;

        const deleteMessageResult = sqsExtendedClient.deleteMessage({
            QueueUrl: queueUrl,
            ReceiptHandle: messages3[0].ReceiptHandle,
        })
        'promise' in deleteMessageResult ? await deleteMessageResult.promise() : await deleteMessageResult;

        // Then
        expect(messages1.length).toBe(1);
        expect(messages2).toBe(undefined);
        expect(messages3.length).toBe(1);
    });

    it('should delete message', async () => {
        // Given
        const sqsExtendedClientSend = new ExtendedSqsClient(sqs, s3, { bucketName });
        const sqsExtendedClient = new ExtendedSqsClient(sqs, s3);

        // When
        const sendMessageResult = sqsExtendedClientSend.sendMessage({
            QueueUrl: queueUrl,
            MessageBody: largeMessageBody,
        })
        'promise' in sendMessageResult ? await sendMessageResult.promise() : await sendMessageResult;

        const receiveMessageResult1 = sqsExtendedClient.receiveMessage({
            QueueUrl: queueUrl,
        })
        const { Messages: messages1 } = 'promise' in receiveMessageResult1 ? await receiveMessageResult1.promise() : await receiveMessageResult1;

        const changeVisibilityResult = sqsExtendedClient.changeMessageVisibility({
            QueueUrl: queueUrl,
            ReceiptHandle: messages1[0].ReceiptHandle,
            VisibilityTimeout: 0,
        })
        'promise' in changeVisibilityResult ? await changeVisibilityResult.promise() : await changeVisibilityResult;

        const receiveMessageResult2 = sqsExtendedClient.receiveMessage({
            QueueUrl: queueUrl,
        })
        const { Messages: messages2 } = 'promise' in receiveMessageResult2 ? await receiveMessageResult2.promise() : await receiveMessageResult2;

        const deleteMessageResult = sqsExtendedClient.deleteMessage({
            QueueUrl: queueUrl,
            ReceiptHandle: messages2[0].ReceiptHandle,
        })
        'promise' in deleteMessageResult ? await deleteMessageResult.promise() : await deleteMessageResult;

        // expect message to be deleted
        const receiveMessageResult3 = sqsExtendedClient.receiveMessage({
            QueueUrl: queueUrl,
        })
        const { Messages: messages3 } = 'promise' in receiveMessageResult3 ? await receiveMessageResult3.promise() : await receiveMessageResult3;

        // Then
        expect(messages1.length).toBe(1);
        expect(messages2.length).toBe(1);
        expect(messages3).toBe(undefined);
    });

    it('should delete message batch', async () => {
        // Given
        const sqsExtendedClientSend = new ExtendedSqsClient(sqs, s3, { bucketName });
        const sqsExtendedClient = new ExtendedSqsClient(sqs, s3);

        // When
        const sendMessageResult = sqsExtendedClientSend.sendMessageBatch({
            QueueUrl: queueUrl,
            Entries: [
                {
                    Id: '1',
                    MessageBody: largeMessageBody,
                },
                {
                    Id: '2',
                    MessageBody: 'small body',
                },
            ],
        })
        'promise' in sendMessageResult ? await sendMessageResult.promise() : await sendMessageResult;

        const receiveMessageResult1 = sqsExtendedClient.receiveMessage({
            QueueUrl: queueUrl,
            MaxNumberOfMessages: 10,
        })
        const { Messages: messages1 } = 'promise' in receiveMessageResult1 ? await receiveMessageResult1.promise() : await receiveMessageResult1;

        const changeVisibilityResult1 = sqsExtendedClient.changeMessageVisibility({
            QueueUrl: queueUrl,
            ReceiptHandle: messages1[0].ReceiptHandle,
            VisibilityTimeout: 0,
        })
        'promise' in changeVisibilityResult1 ? await changeVisibilityResult1.promise() : await changeVisibilityResult1;
        const changeVisibilityResult2 = sqsExtendedClient.changeMessageVisibility({
            QueueUrl: queueUrl,
            ReceiptHandle: messages1[1].ReceiptHandle,
            VisibilityTimeout: 0,
        })
        'promise' in changeVisibilityResult2 ? await changeVisibilityResult2.promise() : await changeVisibilityResult2;

        const receiveMessageResult2 = sqsExtendedClient.receiveMessage({
            QueueUrl: queueUrl,
            MaxNumberOfMessages: 10,
        })
        const { Messages: messages2 } = 'promise' in receiveMessageResult2 ? await receiveMessageResult2.promise() : await receiveMessageResult2;

        const deleteMessageResult = sqsExtendedClient.deleteMessageBatch({
            QueueUrl: queueUrl,
            Entries: [
                {
                    Id: '1',
                    ReceiptHandle: messages2[0].ReceiptHandle,
                },
                {
                    Id: '2',
                    ReceiptHandle: messages2[1].ReceiptHandle,
                },
            ],
        })
        'promise' in deleteMessageResult ? await deleteMessageResult.promise() : await deleteMessageResult;

        // expect 2 messages to be deleted
        const receiveMessageResult3 = sqsExtendedClient.receiveMessage({
            QueueUrl: queueUrl,
            MaxNumberOfMessages: 10,
        })
        const { Messages: messages3 } = 'promise' in receiveMessageResult3 ? await receiveMessageResult3.promise() : await receiveMessageResult3;

        // Then
        expect(messages1.length).toBe(2);
        expect(messages2.length).toBe(2);
        expect(messages3).toBe(undefined);
    });
});
