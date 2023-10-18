const { mockClient } = require('aws-sdk-client-mock');
require('aws-sdk-client-mock-jest');
const {
    SQSClient,
    SendMessageCommand,
    SendMessageBatchCommand,
    ReceiveMessageCommand,
    DeleteMessageCommand,
    DeleteMessageBatchCommand,
    ChangeMessageVisibilityCommand,
    ChangeMessageVisibilityBatchCommand,
} = require('@aws-sdk/client-sqs');
const { S3Client, PutObjectCommand, GetObjectCommand, DeleteObjectCommand } = require('@aws-sdk/client-s3');
const { sdkStreamMixin } = require('@smithy/util-stream');
const { Readable } = require('stream');
const ExtendedSqsClient = require('../ExtendedSqsClient');

const mockSQS = mockClient(SQSClient);
const mockS3 = mockClient(S3Client);
const mockS3Key = '1234-5678';

jest.mock('uuid', () => ({
    v4: () => mockS3Key,
}));

const testMessageAttribute = {
    DataType: 'String',
    StringValue: 'attr value',
};
const s3MessageBodyKeyAttribute = {
    DataType: 'String',
    StringValue: `(test-bucket)${mockS3Key}`,
};

afterEach(() => {
    mockS3.reset();
    mockSQS.reset();
});

describe('ExtendedSqsClient sendMessage', () => {
    it('should send small message directly', async () => {
        mockSQS.on(SendMessageCommand).resolves({ MessageId: 'test-id' });

        const client = new ExtendedSqsClient({ bucketName: 'test-bucket' });

        // When
        await client.sendMessage({
            QueueUrl: 'test-queue',
            MessageAttributes: { MessageAttribute: testMessageAttribute },
            MessageBody: 'small message body',
        });

        // Then
        expect(mockSQS).toHaveReceivedCommandTimes(SendMessageCommand, 1);
        expect(mockSQS).toHaveReceivedCommandWith(SendMessageCommand, {
            QueueUrl: 'test-queue',
            MessageBody: 'small message body',
            MessageAttributes: { MessageAttribute: testMessageAttribute },
        });
    });

    it('should use S3 to send large message (using promise())', async () => {
        // Given
        const sqsResponse = { MessageId: 'test message id' };
        mockSQS.on(SendMessageCommand).resolves(sqsResponse);

        const client = new ExtendedSqsClient({ bucketName: 'test-bucket' });
        const largeMessageBody = 'body'.repeat(1024 * 1024);

        // When
        const result = await client.sendMessage({
            QueueUrl: 'test-queue',
            MessageAttributes: { MessageAttribute: testMessageAttribute },
            MessageBody: largeMessageBody,
        });

        // Then
        expect(result).toEqual(sqsResponse);

        expect(mockS3).toHaveReceivedCommandTimes(PutObjectCommand, 1);
        expect(mockS3).toHaveReceivedCommandWith(PutObjectCommand, {
            Bucket: 'test-bucket',
            Key: mockS3Key,
            Body: largeMessageBody,
        });

        expect(mockSQS).toHaveReceivedCommandTimes(SendMessageCommand, 1);
        expect(mockSQS).toHaveReceivedCommandWith(SendMessageCommand, {
            QueueUrl: 'test-queue',
            MessageBody: mockS3Key,
            MessageAttributes: {
                MessageAttribute: testMessageAttribute,
                S3MessageBodyKey: s3MessageBodyKeyAttribute,
            },
        });
    });

    it('should use s3 to send small message when alwaysUseS3=true', async () => {
        // Given
        mockSQS.on(SendMessageCommand).resolves({ MessageId: 'test-id' });

        const client = new ExtendedSqsClient({
            bucketName: 'test-bucket',
            alwaysUseS3: true,
        });

        // When
        await client.sendMessage({
            QueueUrl: 'test-queue',
            MessageAttributes: { MessageAttribute: testMessageAttribute },
            MessageBody: 'small message body',
        });

        // Then
        expect(mockS3).toHaveReceivedCommandTimes(PutObjectCommand, 1);
        expect(mockS3).toHaveReceivedCommandWith(PutObjectCommand, {
            Bucket: 'test-bucket',
            Key: mockS3Key,
            Body: 'small message body',
        });

        expect(mockSQS).toHaveReceivedCommandTimes(SendMessageCommand, 1);
        expect(mockSQS).toHaveReceivedCommandWith(SendMessageCommand, {
            QueueUrl: 'test-queue',
            MessageBody: mockS3Key,
            MessageAttributes: {
                MessageAttribute: testMessageAttribute,
                S3MessageBodyKey: s3MessageBodyKeyAttribute,
            },
        });
    });

    it('should apply custom sendTransform function', async () => {
        // Given
        mockSQS.on(SendMessageCommand).resolves({ MessageId: 'test-id' });

        const client = new ExtendedSqsClient({
            bucketName: 'test-bucket',
            sendTransform: (message) => ({
                messageBody: `custom ${message.MessageBody}`,
                s3Content: 'custom s3 content',
            }),
        });

        // When
        await client.sendMessage({
            QueueUrl: 'test-queue',
            MessageBody: 'message body',
        });

        // Then
        expect(mockS3).toHaveReceivedCommandTimes(PutObjectCommand, 1);
        expect(mockS3).toHaveReceivedCommandWith(PutObjectCommand, {
            Bucket: 'test-bucket',
            Key: mockS3Key,
            Body: 'custom s3 content',
        });

        expect(mockSQS).toHaveReceivedCommandTimes(SendMessageCommand, 1);
        expect(mockSQS).toHaveReceivedCommandWith(SendMessageCommand, {
            QueueUrl: 'test-queue',
            MessageBody: 'custom message body',
            MessageAttributes: {
                S3MessageBodyKey: s3MessageBodyKeyAttribute,
            },
        });
    });

    it('should respect existing S3MessageBodyKey', async () => {
        // Given
        mockSQS.on(SendMessageCommand).resolves({ MessageId: 'test-id' });

        const client = new ExtendedSqsClient({
            bucketName: 'test-bucket',
            alwaysUseS3: true,
        });

        // When
        await client.sendMessage({
            QueueUrl: 'test-queue',
            MessageAttributes: { S3MessageBodyKey: s3MessageBodyKeyAttribute },
            MessageBody: 'message body',
        });

        // Then
        expect(mockSQS).toHaveReceivedCommandTimes(SendMessageCommand, 1);
        expect(mockSQS).toHaveReceivedCommandWith(SendMessageCommand, {
            QueueUrl: 'test-queue',
            MessageBody: s3MessageBodyKeyAttribute.StringValue,
            MessageAttributes: {
                S3MessageBodyKey: s3MessageBodyKeyAttribute,
            },
        });

        expect(mockS3).toHaveReceivedCommandTimes(PutObjectCommand, 0);
    });

    it('should throw SQS error', async () => {
        // Given
        mockSQS.on(SendMessageCommand).rejects(new Error('test error'));

        const client = new ExtendedSqsClient({ bucketName: 'test-bucket' });

        // When
        let result;

        try {
            await client.sendMessage({
                QueueUrl: 'test-queue',
                MessageAttributes: { MessageAttribute: testMessageAttribute },
                MessageBody: 'body'.repeat(1024 * 1024),
            });
        } catch (err) {
            result = err;
        }

        // Then
        expect(result.message).toEqual('test error');
    });

    it('should throw S3 error', async () => {
        // Given
        mockS3.on(PutObjectCommand).rejects(new Error('test error'));

        const client = new ExtendedSqsClient({ bucketName: 'test-bucket' });

        // When
        let result;

        try {
            await client.sendMessage({
                QueueUrl: 'test-queue',
                MessageAttributes: { MessageAttribute: testMessageAttribute },
                MessageBody: 'body'.repeat(1024 * 1024),
            });
        } catch (err) {
            result = err;
        }

        // Then
        expect(result.message).toEqual('test error');
    });

    it('should throw missing required bucketName error', async () => {
        // Given
        const client = new ExtendedSqsClient({});

        // When
        let result;

        try {
            await client.sendMessage({
                QueueUrl: 'test-queue',
                MessageAttributes: { MessageAttribute: testMessageAttribute },
                MessageBody: 'body'.repeat(1024 * 1024),
            });
        } catch (err) {
            result = err;
        }

        // Then
        expect(result.message).toEqual('bucketName option is required for sending messages');
    });
});

describe('ExtendedSqsClient sendMessageBatch', () => {
    it('should batch send messages', async () => {
        // Given
        mockSQS.on(SendMessageBatchCommand).resolves({
            // SendMessageBatchResult
            Successful: [
                // SendMessageBatchResultEntryList // required
                {
                    // SendMessageBatchResultEntry
                    Id: 'STRING_VALUE', // required
                    MessageId: 'STRING_VALUE', // required
                    MD5OfMessageBody: 'STRING_VALUE', // required
                    MD5OfMessageAttributes: 'STRING_VALUE',
                    MD5OfMessageSystemAttributes: 'STRING_VALUE',
                    SequenceNumber: 'STRING_VALUE',
                },
            ],
            Failed: [
                // BatchResultErrorEntryList // required
                {
                    // BatchResultErrorEntry
                    Id: 'STRING_VALUE', // required
                    SenderFault: true, // required
                    Code: 'STRING_VALUE', // required
                    Message: 'STRING_VALUE',
                },
            ],
        });

        const client = new ExtendedSqsClient({ bucketName: 'test-bucket' });

        const largeMessageBody1 = 'body1'.repeat(1024 * 1024);
        const largeMessageBody2 = 'body2'.repeat(1024 * 1024);

        // When
        await client.sendMessageBatch({
            QueueUrl: 'test-queue',
            Entries: [
                {
                    Id: '1',
                    MessageBody: largeMessageBody1,
                },
                {
                    Id: '2',
                    MessageBody: 'small message body',
                },
                {
                    Id: '3',
                    MessageBody: largeMessageBody2,
                },
            ],
        });

        // Then
        expect(mockS3).toHaveReceivedCommandTimes(PutObjectCommand, 2);
        expect(mockS3).toHaveReceivedNthCommandWith(1, PutObjectCommand, {
            Bucket: 'test-bucket',
            Key: mockS3Key,
            Body: largeMessageBody1,
        });
        expect(mockS3).toHaveReceivedNthCommandWith(2, PutObjectCommand, {
            Bucket: 'test-bucket',
            Key: mockS3Key,
            Body: largeMessageBody2,
        });

        expect(mockSQS).toHaveReceivedCommandTimes(SendMessageBatchCommand, 1);
        expect(mockSQS).toHaveReceivedCommandWith(SendMessageBatchCommand, {
            QueueUrl: 'test-queue',
            Entries: [
                {
                    Id: '1',
                    MessageBody: mockS3Key,
                    MessageAttributes: { S3MessageBodyKey: s3MessageBodyKeyAttribute },
                },
                {
                    Id: '2',
                    MessageBody: 'small message body',
                },
                {
                    Id: '3',
                    MessageBody: mockS3Key,
                    MessageAttributes: { S3MessageBodyKey: s3MessageBodyKeyAttribute },
                },
            ],
        });
    });

    it('should throw missing required bucketName error', async () => {
        // Given
        const client = new ExtendedSqsClient({});

        const largeMessageBody1 = 'body1'.repeat(1024 * 1024);

        // When
        let error;
        try {
            await client.sendMessageBatch({
                QueueUrl: 'test-queue',
                Entries: [
                    {
                        Id: '1',
                        MessageBody: largeMessageBody1,
                    },
                    {
                        Id: '2',
                        MessageBody: 'small message body',
                    },
                ],
            });
        } catch (err) {
            error = err;
        }

        // Then
        expect(error.message).toEqual('bucketName option is required for sending messages');
    });
});

describe('ExtendedSqsClient receiveMessage', () => {
    it('should receive a message not using S3', async () => {
        // Given
        const messages = {
            Messages: [
                {
                    Body: 'message body',
                    ReceiptHandle: 'receipthandle',
                    MessageAttributes: {
                        MessageAttribute: testMessageAttribute,
                    },
                },
            ],
        };
        mockSQS.on(ReceiveMessageCommand).resolves(messages);

        const client = new ExtendedSqsClient({});

        // When
        const response = await client.receiveMessage({
            QueueUrl: 'test-queue',
            MessageAttributeNames: ['MessageAttribute'],
        });

        // Then
        expect(mockSQS).toHaveReceivedCommandTimes(ReceiveMessageCommand, 1);
        expect(mockSQS).toHaveReceivedCommandWith(ReceiveMessageCommand, {
            MessageAttributeNames: ['MessageAttribute', 'S3MessageBodyKey'],
            QueueUrl: 'test-queue',
        });

        expect(response).toEqual(messages);
    });

    it('should receive a message using S3', async () => {
        // Given
        const messages = {
            Messages: [
                {
                    Body: '8765-4321',
                    ReceiptHandle: 'receipthandle',
                    MessageAttributes: {
                        MessageAttribute: testMessageAttribute,
                        S3MessageBodyKey: s3MessageBodyKeyAttribute,
                    },
                },
            ],
        };
        mockSQS.on(ReceiveMessageCommand).resolves(messages);

        const streamMock = new Readable();
        streamMock.push('message body');
        streamMock.push(null); // end of stream
        const sdkStreamMock = sdkStreamMixin(streamMock);

        const s3Content = {
            Body: sdkStreamMock,
        };
        mockS3.on(GetObjectCommand).resolves(s3Content);

        const client = new ExtendedSqsClient();

        // When
        const response = await client.receiveMessage({
            QueueUrl: 'test-queue',
            MessageAttributeNames: ['MessageAttribute'],
        });

        // Then
        expect(mockSQS).toHaveReceivedCommandTimes(ReceiveMessageCommand, 1);
        expect(mockSQS).toHaveReceivedCommandWith(ReceiveMessageCommand, {
            MessageAttributeNames: ['MessageAttribute', 'S3MessageBodyKey'],
            QueueUrl: 'test-queue',
        });

        expect(mockS3).toHaveReceivedCommandTimes(GetObjectCommand, 1);
        expect(mockS3).toHaveReceivedCommandWith(GetObjectCommand, {
            Bucket: 'test-bucket',
            Key: mockS3Key,
        });

        expect(response).toEqual({
            Messages: [
                {
                    Body: 'message body',
                    ReceiptHandle: `-..s3BucketName..-test-bucket-..s3BucketName..--..s3Key..-${mockS3Key}-..s3Key..-receipthandle`,
                    MessageAttributes: {
                        MessageAttribute: testMessageAttribute,
                        S3MessageBodyKey: s3MessageBodyKeyAttribute,
                    },
                },
            ],
        });
    });

    it('should apply custom receiveTransform function', async () => {
        // Given
        const messages = {
            Messages: [
                {
                    Body: 'custom message body',
                    ReceiptHandle: 'receipthandle',
                    MessageAttributes: {
                        S3MessageBodyKey: s3MessageBodyKeyAttribute,
                    },
                },
            ],
        };
        mockSQS.on(ReceiveMessageCommand).resolves(messages);

        const streamMock = new Readable();
        streamMock.push('custom s3 content');
        streamMock.push(null); // end of stream
        const sdkStreamMock = sdkStreamMixin(streamMock);

        const mockS3Content = {
            Body: sdkStreamMock,
        };
        mockS3.on(GetObjectCommand).resolves(mockS3Content);

        const client = new ExtendedSqsClient({
            receiveTransform: (message, s3Content) => `${message.Body} - ${s3Content}`,
        });

        // When
        const response = await client.receiveMessage({
            QueueUrl: 'test-queue',
        });

        // Then
        expect(mockSQS).toHaveReceivedCommandTimes(ReceiveMessageCommand, 1);
        expect(mockSQS).toHaveReceivedCommandWith(ReceiveMessageCommand, {
            MessageAttributeNames: ['S3MessageBodyKey'],
            QueueUrl: 'test-queue',
        });

        expect(mockS3).toHaveReceivedCommandTimes(GetObjectCommand, 1);
        expect(mockS3).toHaveReceivedCommandWith(GetObjectCommand, {
            Bucket: 'test-bucket',
            Key: mockS3Key,
        });

        expect(response).toEqual({
            Messages: [
                {
                    Body: 'custom message body - custom s3 content',
                    ReceiptHandle: `-..s3BucketName..-test-bucket-..s3BucketName..--..s3Key..-${mockS3Key}-..s3Key..-receipthandle`,
                    MessageAttributes: {
                        S3MessageBodyKey: s3MessageBodyKeyAttribute,
                    },
                },
            ],
        });
    });

    it('should throw SQS error', async () => {
        // Given
        mockSQS.on(ReceiveMessageCommand).rejects(new Error('test error'));

        const client = new ExtendedSqsClient({ bucketName: 'test-bucket' });

        // When
        let result;

        try {
            await client.receiveMessage({
                QueueUrl: 'test-queue',
                MessageAttributeNames: ['MessageAttribute'],
            });
        } catch (err) {
            result = err;
        }

        // Then
        expect(result.message).toEqual('test error');
    });

    it('should throw S3 error', async () => {
        // Given
        const messages = {
            Messages: [
                {
                    Body: '8765-4321',
                    ReceiptHandle: 'receipthandle',
                    MessageAttributes: {
                        MessageAttribute: testMessageAttribute,
                        S3MessageBodyKey: s3MessageBodyKeyAttribute,
                    },
                },
            ],
        };
        mockSQS.on(ReceiveMessageCommand).resolves(messages);
        mockS3.on(GetObjectCommand).rejects(new Error('test error'));

        const client = new ExtendedSqsClient({ bucketName: 'test-bucket' });

        // When
        let result;

        try {
            await client.receiveMessage({
                QueueUrl: 'test-queue',
                MessageAttributeNames: ['MessageAttribute'],
            });
        } catch (err) {
            result = err;
        }

        // Then
        expect(result.message).toEqual('test error');
    });

    it('should process empty response', async () => {
        // Given
        const messages = {};
        mockSQS.on(ReceiveMessageCommand).resolves(messages);

        const client = new ExtendedSqsClient({});

        // When
        const response = await client.receiveMessage({
            QueueUrl: 'test-queue',
            MessageAttributeNames: ['MessageAttribute'],
        });

        // Then
        expect(mockSQS).toHaveReceivedCommandTimes(ReceiveMessageCommand, 1);
        expect(mockSQS).toHaveReceivedCommandWith(ReceiveMessageCommand, {
            MessageAttributeNames: ['MessageAttribute', 'S3MessageBodyKey'],
            QueueUrl: 'test-queue',
        });

        expect(response).toEqual(messages);
    });
});

describe('ExtendedSqsClient deleteMessage', () => {
    it('should delete a message not using S3', async () => {
        // Given
        mockSQS.on(DeleteMessageCommand).resolves({});

        const client = new ExtendedSqsClient({ bucketName: 'test-bucket' });

        // When
        await client.deleteMessage({
            QueueUrl: 'test-queue',
            ReceiptHandle: 'receipthandle',
        });

        // Then
        expect(mockSQS).toHaveReceivedCommandTimes(DeleteMessageCommand, 1);
        expect(mockSQS).toHaveReceivedCommandWith(DeleteMessageCommand, {
            QueueUrl: 'test-queue',
            ReceiptHandle: 'receipthandle',
        });
    });

    it('should delete a message and S3 message content', async () => {
        // Given
        mockSQS.on(DeleteMessageCommand).resolves({});
        mockS3.on(DeleteObjectCommand).resolves({
            DeleteMarker: true,
            VersionId: 'STRING_VALUE',
            RequestCharged: 'requester',
        });

        const client = new ExtendedSqsClient({ bucketName: 'test-bucket' });

        // When
        await client.deleteMessage({
            QueueUrl: 'test-queue',
            ReceiptHandle: `-..s3BucketName..-test-bucket-..s3BucketName..--..s3Key..-${mockS3Key}-..s3Key..-receipthandle`,
        });

        // Then
        expect(mockSQS).toHaveReceivedCommandTimes(DeleteMessageCommand, 1);
        expect(mockSQS).toHaveReceivedCommandWith(DeleteMessageCommand, {
            QueueUrl: 'test-queue',
            ReceiptHandle: 'receipthandle',
        });

        expect(mockS3).toHaveReceivedCommandTimes(DeleteObjectCommand, 1);
        expect(mockS3).toHaveReceivedCommandWith(DeleteObjectCommand, {
            Bucket: 'test-bucket',
            Key: mockS3Key,
        });
    });
});

describe('ExtendedSqsClient deleteMessageBatch', () => {
    it('should batch delete messages', async () => {
        // Given
        mockSQS.on(DeleteMessageBatchCommand).resolves({
            Successful: [
                {
                    Id: 'STRING_VALUE', // required
                },
            ],
        });
        mockS3.on(DeleteObjectCommand).resolves({
            DeleteMarker: true,
            VersionId: 'STRING_VALUE',
            RequestCharged: 'requester',
        });

        const client = new ExtendedSqsClient({ bucketName: 'test-bucket' });

        // When
        await client.deleteMessageBatch({
            QueueUrl: 'test-queue',
            Entries: [
                {
                    Id: '1',
                    ReceiptHandle: `-..s3BucketName..-test-bucket-..s3BucketName..--..s3Key..-${mockS3Key}1-..s3Key..-receipthandle1`,
                },
                {
                    Id: '2',
                    ReceiptHandle: 'receipthandle2',
                },
                {
                    Id: '1',
                    ReceiptHandle: `-..s3BucketName..-test-bucket-..s3BucketName..--..s3Key..-${mockS3Key}2-..s3Key..-receipthandle3`,
                },
            ],
        });

        // Then
        expect(mockSQS).toHaveReceivedCommandTimes(DeleteMessageBatchCommand, 1);
        expect(mockSQS).toHaveReceivedCommandWith(DeleteMessageBatchCommand, {
            QueueUrl: 'test-queue',
            Entries: [
                {
                    Id: '1',
                    ReceiptHandle: `receipthandle1`,
                },
                {
                    Id: '2',
                    ReceiptHandle: 'receipthandle2',
                },
                {
                    Id: '1',
                    ReceiptHandle: `receipthandle3`,
                },
            ],
        });

        expect(mockS3).toHaveReceivedCommandTimes(DeleteObjectCommand, 2);
        expect(mockS3).toHaveReceivedNthCommandWith(1, DeleteObjectCommand, {
            Bucket: 'test-bucket',
            Key: `${mockS3Key}1`,
        });
        expect(mockS3).toHaveReceivedNthCommandWith(2, DeleteObjectCommand, {
            Bucket: 'test-bucket',
            Key: `${mockS3Key}2`,
        });
    });
});

describe('ExtendedSqsClient changeMessageVisibility', () => {
    it('should change visibility of a message not using S3', async () => {
        // Given
        mockSQS.on(ChangeMessageVisibilityCommand).resolves({});

        const client = new ExtendedSqsClient({ bucketName: 'test-bucket' });

        // When
        await client.changeMessageVisibility({
            QueueUrl: 'test-queue',
            ReceiptHandle: 'receipthandle',
        });

        // Then
        expect(mockSQS).toHaveReceivedCommandTimes(ChangeMessageVisibilityCommand, 1);
        expect(mockSQS).toHaveReceivedCommandWith(ChangeMessageVisibilityCommand, {
            QueueUrl: 'test-queue',
            ReceiptHandle: 'receipthandle',
        });
    });

    it('should change visibility of a message using S3', async () => {
        // Given
        mockSQS.on(ChangeMessageVisibilityCommand).resolves({});
        // const sqs = {
        //     changeMessageVisibility: jest.fn(() => ({ promise: () => Promise.resolve('success') })),
        // };

        const client = new ExtendedSqsClient({ bucketName: 'test-bucket' });

        // When
        await client.changeMessageVisibility({
            QueueUrl: 'test-queue',
            ReceiptHandle: `-..s3BucketName..-test-bucket-..s3BucketName..--..s3Key..-${mockS3Key}-..s3Key..-receipthandle`,
        });

        // Then
        expect(mockSQS).toHaveReceivedCommandTimes(ChangeMessageVisibilityCommand, 1);
        expect(mockSQS).toHaveReceivedCommandWith(ChangeMessageVisibilityCommand, {
            QueueUrl: 'test-queue',
            ReceiptHandle: 'receipthandle',
        });
    });
});

describe('ExtendedSqsClient changeMessageVisibilityBatch', () => {
    it('should batch change visibility', async () => {
        // Given
        mockSQS.on(ChangeMessageVisibilityBatchCommand).resolves({
            Successful: [
                {
                    Id: 'STRING_VALUE',
                },
            ],
        });

        const client = new ExtendedSqsClient({ bucketName: 'test-bucket' });

        // When
        await client.changeMessageVisibilityBatch({
            QueueUrl: 'test-queue',
            Entries: [
                {
                    Id: '1',
                    ReceiptHandle: `-..s3BucketName..-test-bucket-..s3BucketName..--..s3Key..-${mockS3Key}-..s3Key..-receipthandle1`,
                },
                {
                    Id: '2',
                    ReceiptHandle: `-..s3BucketName..-test-bucket-..s3BucketName..--..s3Key..-${mockS3Key}-..s3Key..-receipthandle2`,
                },
            ],
        });

        // Then
        expect(mockSQS).toHaveReceivedCommandTimes(ChangeMessageVisibilityBatchCommand, 1);
        expect(mockSQS).toHaveReceivedCommandWith(ChangeMessageVisibilityBatchCommand, {
            QueueUrl: 'test-queue',
            Entries: [
                {
                    Id: '1',
                    ReceiptHandle: `receipthandle1`,
                },
                {
                    Id: '2',
                    ReceiptHandle: `receipthandle2`,
                },
            ],
        });
    });
});

describe('ExtendedSqsClient middleware', () => {
    it('should handle a message not using S3', async () => {
        // Given
        const handler = {
            event: {
                Records: [
                    {
                        body: 'message body',
                        receiptHandle: 'receipthandle',
                        messageAttributes: {
                            messageAttribute: testMessageAttribute,
                        },
                    },
                ],
            },
        };

        const middleware = new ExtendedSqsClient({ bucketName: 'test-bucket' }).middleware();

        // When
        await middleware.before(handler);

        // Then
        expect(handler.event.Records).toEqual([
            {
                body: 'message body',
                receiptHandle: 'receipthandle',
                messageAttributes: {
                    messageAttribute: testMessageAttribute,
                },
            },
        ]);
    });

    it('should handle a message using S3', async () => {
        // Given
        const handler = {
            event: {
                Records: [
                    {
                        body: 'message body',
                        receiptHandle: 'receipthandle',
                        messageAttributes: {
                            MessageAttribute: testMessageAttribute,
                            S3MessageBodyKey: s3MessageBodyKeyAttribute,
                        },
                    },
                ],
            },
        };

        const streamMock = new Readable();
        streamMock.push('message body');
        streamMock.push(null); // end of stream
        const sdkStreamMock = sdkStreamMixin(streamMock);

        const s3Content = {
            Body: sdkStreamMock,
        };
        mockS3.on(GetObjectCommand).resolves(s3Content);

        const middleware = new ExtendedSqsClient({ bucketName: 'test-bucket' }).middleware();

        // When
        await middleware.before(handler);

        // Then
        expect(mockS3).toHaveReceivedCommandTimes(GetObjectCommand, 1);
        expect(mockS3).toHaveReceivedCommandWith(GetObjectCommand, {
            Bucket: 'test-bucket',
            Key: mockS3Key,
        });

        expect(handler.event.Records).toEqual([
            {
                body: 'message body',
                receiptHandle: `-..s3BucketName..-test-bucket-..s3BucketName..--..s3Key..-${mockS3Key}-..s3Key..-receipthandle`,
                messageAttributes: {
                    MessageAttribute: testMessageAttribute,
                    S3MessageBodyKey: s3MessageBodyKeyAttribute,
                },
            },
        ]);
    });

    it('should apply custom receiveTransform function', async () => {
        // Given
        const handler = {
            event: {
                Records: [
                    {
                        body: 'custom message body',
                        receiptHandle: 'receipthandle',
                        messageAttributes: {
                            MessageAttribute: testMessageAttribute,
                            S3MessageBodyKey: s3MessageBodyKeyAttribute,
                        },
                    },
                ],
            },
        };

        const streamMock = new Readable();
        streamMock.push('custom s3 content');
        streamMock.push(null); // end of stream
        const sdkStreamMock = sdkStreamMixin(streamMock);

        const s3Response = {
            Body: sdkStreamMock,
        };
        mockS3.on(GetObjectCommand).resolves(s3Response);

        const middleware = new ExtendedSqsClient({
            bucketName: 'test-bucket',
            receiveTransform: (message, s3Content) => `${message.body} - ${s3Content}`,
        }).middleware();

        // When
        await middleware.before(handler);

        // Then
        expect(mockS3).toHaveReceivedCommandTimes(GetObjectCommand, 1);
        expect(mockS3).toHaveReceivedCommandWith(GetObjectCommand, {
            Bucket: 'test-bucket',
            Key: mockS3Key,
        });

        expect(handler.event.Records).toEqual([
            {
                body: 'custom message body - custom s3 content',
                receiptHandle: `-..s3BucketName..-test-bucket-..s3BucketName..--..s3Key..-${mockS3Key}-..s3Key..-receipthandle`,
                messageAttributes: {
                    MessageAttribute: testMessageAttribute,
                    S3MessageBodyKey: s3MessageBodyKeyAttribute,
                },
            },
        ]);
    });

    it('should handle a invalid message attribute', async () => {
        // Given
        const handler = {
            event: {
                Records: [
                    {
                        body: 'message body',
                        receiptHandle: 'receipthandle',
                        messageAttributes: {
                            messageAttribute: testMessageAttribute,
                            S3MessageBodyKey: {},
                        },
                    },
                ],
            },
        };

        const middleware = new ExtendedSqsClient({ bucketName: 'test-bucket' }).middleware();

        // When
        let error;
        try {
            await middleware.before(handler);
        } catch (err) {
            error = err;
        }

        // Then
        expect(error.message).toEqual('Invalid S3MessageBodyKey message attribute: Missing stringValue/StringValue');
    });

    it('should handle a message without message attributes', async () => {
        // Given
        const handler = {
            event: {
                Records: [
                    {
                        body: 'message body',
                        receiptHandle: 'receipthandle',
                    },
                ],
            },
        };

        const middleware = new ExtendedSqsClient({ bucketName: 'test-bucket' }).middleware();

        // When
        await middleware.before(handler);

        // Then
        expect(handler.event.Records).toEqual([
            {
                body: 'message body',
                receiptHandle: 'receipthandle',
            },
        ]);
    });
});
