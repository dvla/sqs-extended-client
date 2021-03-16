const AWS = require('aws-sdk');
const uuid = require('uuid/v4');

const ExtendedSqsClient = require('../src/ExtendedSqsClient');

const sqsEndpoint = 'http://0.0.0.0:4566';
const sqs = new AWS.SQS({
    apiVersion: '2012-11-05',
    endpoint: sqsEndpoint,
    region: 'eu-west-2',
});

const s3Endpoint = 'http://0.0.0.0:4566';
const s3 = new AWS.S3({
    apiVersion: '2006-03-01',
    endpoint: s3Endpoint,
    region: 'eu-west-2',
    s3ForcePathStyle: true,
});

jest.setTimeout(30000);

let queueUrl;
const bucketName = uuid();

const largeMessageBody = 'large body'.repeat(1024 * 1024);
const largeMessageBody2 = 'large body2'.repeat(1024 * 1024);

beforeEach(async () => {
    queueUrl = (await sqs.createQueue({ QueueName: uuid() }).promise()).QueueUrl;
    await s3.createBucket({ Bucket: bucketName }).promise();
});

afterEach(async () => {
    await sqs.deleteQueue({ QueueUrl: queueUrl }).promise();
    await s3.deleteBucket({ Bucket: bucketName }).promise();
});

async function s3ObjectCount() {
    return (await s3.listObjectsV2({ Bucket: bucketName }).promise()).KeyCount;
}

describe('sqs-extended-client', () => {
    it('should send/receive/delete small message not using S3', async () => {
        // Given
        const sqsExtendedClientSend = new ExtendedSqsClient(sqs, s3, { bucketName });
        const sqsExtendedClient = new ExtendedSqsClient(sqs, s3);

        // When
        await sqsExtendedClientSend
            .sendMessage({
                QueueUrl: queueUrl,
                MessageBody: 'small body',
            })
            .promise();

        const { Messages: messages } = await sqsExtendedClient
            .receiveMessage({
                QueueUrl: queueUrl,
            })
            .promise();

        const s3Objects = await s3ObjectCount();

        await sqsExtendedClient
            .deleteMessage({
                QueueUrl: queueUrl,
                ReceiptHandle: messages[0].ReceiptHandle,
            })
            .promise();

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
        await sqsExtendedClientSend
            .sendMessage({
                QueueUrl: queueUrl,
                MessageBody: largeMessageBody,
            })
            .promise();

        const { Messages: messages } = await sqsExtendedClient
            .receiveMessage({
                QueueUrl: queueUrl,
            })
            .promise();

        const s3Objects = await s3ObjectCount();

        await sqsExtendedClient
            .deleteMessage({
                QueueUrl: queueUrl,
                ReceiptHandle: messages[0].ReceiptHandle,
            })
            .promise();

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
        await sqsExtendedClientSend
            .sendMessage({
                QueueUrl: queueUrl,
                MessageBody: JSON.stringify(bodyObj),
            })
            .promise();

        const { Messages: messages } = await sqsExtendedClient
            .receiveMessage({
                QueueUrl: queueUrl,
            })
            .promise();

        const s3Objects = await s3ObjectCount();

        await sqsExtendedClient
            .deleteMessage({
                QueueUrl: queueUrl,
                ReceiptHandle: messages[0].ReceiptHandle,
            })
            .promise();

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
        await sqsExtendedClientSend
            .sendMessageBatch({
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
            .promise();

        const { Messages: messages } = await sqsExtendedClient
            .receiveMessage({
                QueueUrl: queueUrl,
                MaxNumberOfMessages: 10,
            })
            .promise();

        const s3Objects = await s3ObjectCount();

        await sqsExtendedClient
            .deleteMessageBatch({
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
            .promise();

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
        await sqsExtendedClientSend
            .sendMessage({
                QueueUrl: queueUrl,
                MessageBody: largeMessageBody,
            })
            .promise();

        const { Messages: messages1 } = await sqsExtendedClient
            .receiveMessage({
                QueueUrl: queueUrl,
            })
            .promise();

        // expect message to be invisible
        const { Messages: messages2 } = await sqsExtendedClient
            .receiveMessage({
                QueueUrl: queueUrl,
            })
            .promise();

        await sqsExtendedClient
            .changeMessageVisibility({
                QueueUrl: queueUrl,
                ReceiptHandle: messages1[0].ReceiptHandle,
                VisibilityTimeout: 0,
            })
            .promise();

        // expect message to be visible again
        const { Messages: messages3 } = await sqsExtendedClient
            .receiveMessage({
                QueueUrl: queueUrl,
            })
            .promise();

        await sqsExtendedClient
            .deleteMessage({
                QueueUrl: queueUrl,
                ReceiptHandle: messages3[0].ReceiptHandle,
            })
            .promise();

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
        await sqsExtendedClientSend
            .sendMessage({
                QueueUrl: queueUrl,
                MessageBody: largeMessageBody,
            })
            .promise();

        const { Messages: messages1 } = await sqsExtendedClient
            .receiveMessage({
                QueueUrl: queueUrl,
            })
            .promise();

        await sqsExtendedClient
            .changeMessageVisibility({
                QueueUrl: queueUrl,
                ReceiptHandle: messages1[0].ReceiptHandle,
                VisibilityTimeout: 0,
            })
            .promise();

        const { Messages: messages2 } = await sqsExtendedClient
            .receiveMessage({
                QueueUrl: queueUrl,
            })
            .promise();

        await sqsExtendedClient
            .deleteMessage({
                QueueUrl: queueUrl,
                ReceiptHandle: messages2[0].ReceiptHandle,
            })
            .promise();

        // expect message to be deleted
        const { Messages: messages3 } = await sqsExtendedClient
            .receiveMessage({
                QueueUrl: queueUrl,
            })
            .promise();

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
        await sqsExtendedClientSend
            .sendMessageBatch({
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
            .promise();

        const { Messages: messages1 } = await sqsExtendedClient
            .receiveMessage({
                QueueUrl: queueUrl,
                MaxNumberOfMessages: 10,
            })
            .promise();

        await sqsExtendedClient
            .changeMessageVisibility({
                QueueUrl: queueUrl,
                ReceiptHandle: messages1[0].ReceiptHandle,
                VisibilityTimeout: 0,
            })
            .promise();
        await sqsExtendedClient
            .changeMessageVisibility({
                QueueUrl: queueUrl,
                ReceiptHandle: messages1[1].ReceiptHandle,
                VisibilityTimeout: 0,
            })
            .promise();

        const { Messages: messages2 } = await sqsExtendedClient
            .receiveMessage({
                QueueUrl: queueUrl,
                MaxNumberOfMessages: 10,
            })
            .promise();

        await sqsExtendedClient
            .deleteMessageBatch({
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
            .promise();

        // expect 2 messages to be deleted
        const { Messages: messages3 } = await sqsExtendedClient
            .receiveMessage({
                QueueUrl: queueUrl,
                MaxNumberOfMessages: 10,
            })
            .promise();

        // Then
        expect(messages1.length).toBe(2);
        expect(messages2.length).toBe(2);
        expect(messages3).toBe(undefined);
    });
});
