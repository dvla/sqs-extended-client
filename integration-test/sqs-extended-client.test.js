const { SQSClient, CreateQueueCommand, DeleteQueueCommand } = require('@aws-sdk/client-sqs');
const { S3Client, CreateBucketCommand, DeleteBucketCommand, ListObjectsV2Command } = require('@aws-sdk/client-s3');

const { v4: uuid } = require('uuid');

const ExtendedSqsClient = require('../src/ExtendedSqsClient');

process.env.AWS_ACCESS_KEY_ID = 'test';
process.env.AWS_SECRET_ACCESS_KEY = 'test';

const sqsEndpoint = process.env.SQS_ENDPOINT || 'http://localhost:4566';
const sqsClientConfig = {
    region: 'eu-west-2',
    endpoint: sqsEndpoint,
};
const sqsClient = new SQSClient(sqsClientConfig);

const s3Endpoint = process.env.S3_ENDPOINT || 'http://localhost:4566';
const s3ClientConfig = {
    region: 'eu-west-2',
    endpoint: s3Endpoint,
    forcePathStyle: true,
};
const s3Client = new S3Client(s3ClientConfig);

jest.setTimeout(30000);

let queueUrl;
const bucketName = uuid();

const largeMessageBody = 'large body'.repeat(1024 * 1024);
const largeMessageBody2 = 'large body2'.repeat(1024 * 1024);

beforeEach(async () => {
    queueUrl = (await sqsClient.send(new CreateQueueCommand({ QueueName: uuid() }))).QueueUrl;
    await s3Client.send(new CreateBucketCommand({ Bucket: bucketName }));
});

afterEach(async () => {
    await sqsClient.send(new DeleteQueueCommand({ QueueUrl: queueUrl }));
    await s3Client.send(new DeleteBucketCommand({ Bucket: bucketName }));
});

async function s3ObjectCount() {
    return (await s3Client.send(new ListObjectsV2Command({ Bucket: bucketName }))).KeyCount;
}

describe('sqs-extended-client', () => {
    it('should send/receive/delete small message not using S3', async () => {
        // Given
        const sqsExtendedClientSend = new ExtendedSqsClient({ sqsClientConfig, s3ClientConfig, bucketName });
        const sqsExtendedClient = new ExtendedSqsClient({ sqsClientConfig, s3ClientConfig });

        // When
        await sqsExtendedClientSend.sendMessage({
            QueueUrl: queueUrl,
            MessageBody: 'small body',
        });

        const { Messages: messages } = await sqsExtendedClient.receiveMessage({
            QueueUrl: queueUrl,
        });

        const s3Objects = await s3ObjectCount();

        await sqsExtendedClient.deleteMessage({
            QueueUrl: queueUrl,
            ReceiptHandle: messages[0].ReceiptHandle,
        });

        // Then
        expect(messages[0].Body).toBe('small body');

        expect(s3Objects).toBe(0);
        expect(await s3ObjectCount()).toBe(0);
    });

    it('should send/receive/delete large message using S3', async () => {
        // Given
        const sqsExtendedClientSend = new ExtendedSqsClient({ sqsClientConfig, s3ClientConfig, bucketName });
        const sqsExtendedClient = new ExtendedSqsClient({ sqsClientConfig, s3ClientConfig });

        // When
        await sqsExtendedClientSend.sendMessage({
            QueueUrl: queueUrl,
            MessageBody: largeMessageBody,
        });

        const { Messages: messages } = await sqsExtendedClient.receiveMessage({
            QueueUrl: queueUrl,
        });

        const s3Objects = await s3ObjectCount();

        await sqsExtendedClient.deleteMessage({
            QueueUrl: queueUrl,
            ReceiptHandle: messages[0].ReceiptHandle,
        });

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

        const sqsExtendedClientSend = new ExtendedSqsClient({
            sqsClientConfig,
            s3ClientConfig,
            bucketName,
            sendTransform,
        });
        const sqsExtendedClient = new ExtendedSqsClient({ sqsClientConfig, s3ClientConfig, receiveTransform });

        const bodyObj = { smallItem: 'small', largeItem: 'large'.repeat(1024 * 1024) };

        // When
        await sqsExtendedClientSend.sendMessage({
            QueueUrl: queueUrl,
            MessageBody: JSON.stringify(bodyObj),
        });

        const { Messages: messages } = await sqsExtendedClient.receiveMessage({
            QueueUrl: queueUrl,
        });

        const s3Objects = await s3ObjectCount();

        await sqsExtendedClient.deleteMessage({
            QueueUrl: queueUrl,
            ReceiptHandle: messages[0].ReceiptHandle,
        });

        // Then
        expect(JSON.parse(messages[0].Body)).toEqual(bodyObj);

        expect(s3Objects).toBe(1);
        expect(await s3ObjectCount()).toBe(0);
    });

    it('should send/receive/delete batch using S3', async () => {
        // Given
        const sqsExtendedClientSend = new ExtendedSqsClient({ sqsClientConfig, s3ClientConfig, bucketName });
        const sqsExtendedClient = new ExtendedSqsClient({ sqsClientConfig, s3ClientConfig });

        // When
        await sqsExtendedClientSend.sendMessageBatch({
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
        });

        const { Messages: messages } = await sqsExtendedClient.receiveMessage({
            QueueUrl: queueUrl,
            MaxNumberOfMessages: 10,
        });

        const s3Objects = await s3ObjectCount();

        await sqsExtendedClient.deleteMessageBatch({
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
        });

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
        const sqsExtendedClientSend = new ExtendedSqsClient({ sqsClientConfig, s3ClientConfig, bucketName });
        const sqsExtendedClient = new ExtendedSqsClient({ sqsClientConfig, s3ClientConfig });

        // When
        await sqsExtendedClientSend.sendMessage({
            QueueUrl: queueUrl,
            MessageBody: largeMessageBody,
        });

        const { Messages: messages1 } = await sqsExtendedClient.receiveMessage({
            QueueUrl: queueUrl,
        });

        // expect message to be invisible
        const { Messages: messages2 } = await sqsExtendedClient.receiveMessage({
            QueueUrl: queueUrl,
        });

        await sqsExtendedClient.changeMessageVisibility({
            QueueUrl: queueUrl,
            ReceiptHandle: messages1[0].ReceiptHandle,
            VisibilityTimeout: 0,
        });

        // expect message to be visible again
        const { Messages: messages3 } = await sqsExtendedClient.receiveMessage({
            QueueUrl: queueUrl,
        });

        await sqsExtendedClient.deleteMessage({
            QueueUrl: queueUrl,
            ReceiptHandle: messages3[0].ReceiptHandle,
        });

        // Then
        expect(messages1.length).toBe(1);
        expect(messages2).toBe(undefined);
        expect(messages3.length).toBe(1);
    });

    it('should delete message', async () => {
        // Given
        const sqsExtendedClientSend = new ExtendedSqsClient({ sqsClientConfig, s3ClientConfig, bucketName });
        const sqsExtendedClient = new ExtendedSqsClient({ sqsClientConfig, s3ClientConfig });

        // When
        await sqsExtendedClientSend.sendMessage({
            QueueUrl: queueUrl,
            MessageBody: largeMessageBody,
        });

        const { Messages: messages1 } = await sqsExtendedClient.receiveMessage({
            QueueUrl: queueUrl,
        });

        await sqsExtendedClient.changeMessageVisibility({
            QueueUrl: queueUrl,
            ReceiptHandle: messages1[0].ReceiptHandle,
            VisibilityTimeout: 0,
        });

        const { Messages: messages2 } = await sqsExtendedClient.receiveMessage({
            QueueUrl: queueUrl,
        });

        await sqsExtendedClient.deleteMessage({
            QueueUrl: queueUrl,
            ReceiptHandle: messages2[0].ReceiptHandle,
        });

        // expect message to be deleted
        const { Messages: messages3 } = await sqsExtendedClient.receiveMessage({
            QueueUrl: queueUrl,
        });

        // Then
        expect(messages1.length).toBe(1);
        expect(messages2.length).toBe(1);
        expect(messages3).toBe(undefined);
    });

    it('should delete message batch', async () => {
        // Given
        const sqsExtendedClientSend = new ExtendedSqsClient({ sqsClientConfig, s3ClientConfig, bucketName });
        const sqsExtendedClient = new ExtendedSqsClient({ sqsClientConfig, s3ClientConfig });

        // When
        await sqsExtendedClientSend.sendMessageBatch({
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
        });

        const { Messages: messages1 } = await sqsExtendedClient.receiveMessage({
            QueueUrl: queueUrl,
            MaxNumberOfMessages: 10,
        });

        await sqsExtendedClient.changeMessageVisibility({
            QueueUrl: queueUrl,
            ReceiptHandle: messages1[0].ReceiptHandle,
            VisibilityTimeout: 0,
        });
        await sqsExtendedClient.changeMessageVisibility({
            QueueUrl: queueUrl,
            ReceiptHandle: messages1[1].ReceiptHandle,
            VisibilityTimeout: 0,
        });

        const { Messages: messages2 } = await sqsExtendedClient.receiveMessage({
            QueueUrl: queueUrl,
            MaxNumberOfMessages: 10,
        });

        await sqsExtendedClient.deleteMessageBatch({
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
        });

        // expect 2 messages to be deleted
        const { Messages: messages3 } = await sqsExtendedClient.receiveMessage({
            QueueUrl: queueUrl,
            MaxNumberOfMessages: 10,
        });

        // Then
        expect(messages1.length).toBe(2);
        expect(messages2.length).toBe(2);
        expect(messages3).toBe(undefined);
    });
});
