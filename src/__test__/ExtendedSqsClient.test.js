const ExtendedSqsClient = require('../ExtendedSqsClient');

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

describe('ExtendedSqsClient sendMessage', () => {
    it('should send small message directly', async () => {
        // Given
        const sqs = {
            sendMessage: jest.fn(() => ({ promise: () => Promise.resolve('success') })),
        };

        const client = new ExtendedSqsClient(sqs, {}, { bucketName: 'test-bucket' });

        // When
        await client
            .sendMessage({
                QueueUrl: 'test-queue',
                MessageAttributes: { MessageAttribute: testMessageAttribute },
                MessageBody: 'small message body',
            })
            .promise();

        // Then
        expect(sqs.sendMessage).toHaveBeenCalledTimes(1);
        expect(sqs.sendMessage.mock.calls[0][0]).toEqual({
            QueueUrl: 'test-queue',
            MessageBody: 'small message body',
            MessageAttributes: { MessageAttribute: testMessageAttribute },
        });
    });

    it('should use S3 to send large message (using promise())', async () => {
        // Given
        const sqsResponse = { messageId: 'test message id' };

        const sqs = {
            sendMessage: jest.fn(() => ({ promise: () => Promise.resolve(sqsResponse) })),
        };
        const s3 = {
            putObject: jest.fn(() => ({ promise: () => Promise.resolve() })),
        };

        const client = new ExtendedSqsClient(sqs, s3, { bucketName: 'test-bucket' });
        const largeMessageBody = 'body'.repeat(1024 * 1024);

        // When
        const result = await client
            .sendMessage({
                QueueUrl: 'test-queue',
                MessageAttributes: { MessageAttribute: testMessageAttribute },
                MessageBody: largeMessageBody,
            })
            .promise();

        // Then
        expect(result).toEqual(sqsResponse);

        expect(s3.putObject).toHaveBeenCalledTimes(1);
        expect(s3.putObject.mock.calls[0][0]).toEqual({
            Bucket: 'test-bucket',
            Key: mockS3Key,
            Body: largeMessageBody,
        });

        expect(sqs.sendMessage).toHaveBeenCalledTimes(1);
        expect(sqs.sendMessage.mock.calls[0][0]).toEqual({
            QueueUrl: 'test-queue',
            MessageBody: mockS3Key,
            MessageAttributes: {
                MessageAttribute: testMessageAttribute,
                S3MessageBodyKey: s3MessageBodyKeyAttribute,
            },
        });
    });

    it('should use S3 to send large message (using callback)', async () => {
        // Given
        const sqsResponse = { messageId: 'test message id' };

        const sqs = {
            sendMessage: jest.fn(() => ({ promise: () => Promise.resolve(sqsResponse) })),
        };
        const s3 = {
            putObject: jest.fn(() => ({ promise: () => Promise.resolve() })),
        };

        const client = new ExtendedSqsClient(sqs, s3, { bucketName: 'test-bucket' });
        const largeMessageBody = 'body'.repeat(1024 * 1024);

        // When
        const result = await new Promise((resolve, reject) => {
            const callback = (err, data) => (err ? reject(err) : resolve(data));

            client.sendMessage(
                {
                    QueueUrl: 'test-queue',
                    MessageAttributes: { MessageAttribute: testMessageAttribute },
                    MessageBody: largeMessageBody,
                },
                callback
            );
        });

        // Then
        expect(result).toEqual(sqsResponse);

        expect(s3.putObject).toHaveBeenCalledTimes(1);
        expect(s3.putObject.mock.calls[0][0]).toEqual({
            Bucket: 'test-bucket',
            Key: mockS3Key,
            Body: largeMessageBody,
        });

        expect(sqs.sendMessage).toHaveBeenCalledTimes(1);
        expect(sqs.sendMessage.mock.calls[0][0]).toEqual({
            QueueUrl: 'test-queue',
            MessageBody: mockS3Key,
            MessageAttributes: {
                MessageAttribute: testMessageAttribute,
                S3MessageBodyKey: s3MessageBodyKeyAttribute,
            },
        });
    });

    it('should use S3 to send large message (using send() callback)', async () => {
        // Given
        const sqsResponse = { messageId: 'test message id' };

        const sqs = {
            sendMessage: jest.fn(() => ({ promise: () => Promise.resolve(sqsResponse) })),
        };
        const s3 = {
            putObject: jest.fn(() => ({ promise: () => Promise.resolve() })),
        };

        const client = new ExtendedSqsClient(sqs, s3, { bucketName: 'test-bucket' });
        const largeMessageBody = 'body'.repeat(1024 * 1024);

        // When
        const result = await new Promise((resolve, reject) => {
            const request = client.sendMessage({
                QueueUrl: 'test-queue',
                MessageAttributes: { MessageAttribute: testMessageAttribute },
                MessageBody: largeMessageBody,
            });

            request.send((err, data) => (err ? reject(err) : resolve(data)));
        });

        // Then
        expect(result).toEqual(sqsResponse);

        expect(s3.putObject).toHaveBeenCalledTimes(1);
        expect(s3.putObject.mock.calls[0][0]).toEqual({
            Bucket: 'test-bucket',
            Key: mockS3Key,
            Body: largeMessageBody,
        });

        expect(sqs.sendMessage).toHaveBeenCalledTimes(1);
        expect(sqs.sendMessage.mock.calls[0][0]).toEqual({
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
        const sqs = {
            sendMessage: jest.fn(() => ({ promise: () => Promise.resolve('success') })),
        };
        const s3 = {
            putObject: jest.fn(() => ({ promise: () => Promise.resolve() })),
        };

        const client = new ExtendedSqsClient(sqs, s3, {
            bucketName: 'test-bucket',
            alwaysUseS3: true,
        });

        // When
        await client
            .sendMessage({
                QueueUrl: 'test-queue',
                MessageAttributes: { MessageAttribute: testMessageAttribute },
                MessageBody: 'small message body',
            })
            .promise();

        // Then
        expect(s3.putObject).toHaveBeenCalledTimes(1);
        expect(s3.putObject.mock.calls[0][0]).toEqual({
            Bucket: 'test-bucket',
            Key: mockS3Key,
            Body: 'small message body',
        });

        expect(sqs.sendMessage).toHaveBeenCalledTimes(1);
        expect(sqs.sendMessage.mock.calls[0][0]).toEqual({
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
        const sqs = {
            sendMessage: jest.fn(() => ({ promise: () => Promise.resolve('success') })),
        };
        const s3 = {
            putObject: jest.fn(() => ({ promise: () => Promise.resolve() })),
        };

        const client = new ExtendedSqsClient(sqs, s3, {
            bucketName: 'test-bucket',
            sendTransform: (message) => ({
                messageBody: `custom ${message.MessageBody}`,
                s3Content: 'custom s3 content',
            }),
        });

        // When
        await client
            .sendMessage({
                QueueUrl: 'test-queue',
                MessageBody: 'message body',
            })
            .promise();

        // Then
        expect(s3.putObject).toHaveBeenCalledTimes(1);
        expect(s3.putObject.mock.calls[0][0]).toEqual({
            Bucket: 'test-bucket',
            Key: mockS3Key,
            Body: 'custom s3 content',
        });

        expect(sqs.sendMessage).toHaveBeenCalledTimes(1);
        expect(sqs.sendMessage.mock.calls[0][0]).toEqual({
            QueueUrl: 'test-queue',
            MessageBody: 'custom message body',
            MessageAttributes: {
                S3MessageBodyKey: s3MessageBodyKeyAttribute,
            },
        });
    });

    it('should respect existing S3MessageBodyKey', async () => {
        // Given
        const sqs = {
            sendMessage: jest.fn(() => ({ promise: () => Promise.resolve('success') })),
        };
        const s3 = {
            putObject: jest.fn(() => ({ promise: () => Promise.resolve() })),
        };

        const client = new ExtendedSqsClient(sqs, s3, {
            bucketName: 'test-bucket',
            alwaysUseS3: true,
        });

        // When
        await client
            .sendMessage({
                QueueUrl: 'test-queue',
                MessageAttributes: { S3MessageBodyKey: s3MessageBodyKeyAttribute },
                MessageBody: 'message body',
            })
            .promise();

        // Then
        expect(sqs.sendMessage).toHaveBeenCalledTimes(1);
        expect(sqs.sendMessage.mock.calls[0][0]).toEqual({
            QueueUrl: 'test-queue',
            MessageBody: s3MessageBodyKeyAttribute.StringValue,
            MessageAttributes: {
                S3MessageBodyKey: s3MessageBodyKeyAttribute,
            },
        });

        expect(s3.putObject).toHaveBeenCalledTimes(0);
    });

    it('should throw SQS error (using promise())', async () => {
        // Given
        const sqs = {
            sendMessage: jest.fn(() => ({ promise: () => Promise.reject(new Error('test error')) })),
        };
        const s3 = {
            putObject: jest.fn(() => ({ promise: () => Promise.resolve() })),
        };

        const client = new ExtendedSqsClient(sqs, s3, { bucketName: 'test-bucket' });

        // When
        let result;

        try {
            await client
                .sendMessage({
                    QueueUrl: 'test-queue',
                    MessageAttributes: { MessageAttribute: testMessageAttribute },
                    MessageBody: 'body'.repeat(1024 * 1024),
                })
                .promise()
        } catch (err) {
            result = err;
        }

        // Then
        expect(result.message).toEqual('test error');
    });

    it('should throw SQS error (using callback)', async () => {
        // Given
        const sqs = {
            sendMessage: jest.fn(() => ({ promise: () => Promise.reject(new Error('test error')) })),
        };
        const s3 = {
            putObject: jest.fn(() => ({ promise: () => Promise.resolve() })),
        };

        const client = new ExtendedSqsClient(sqs, s3, { bucketName: 'test-bucket' });

        // When
        let result;

        try {
            await new Promise((resolve, reject) => {
                const callback = (err, data) => (err ? reject(err) : resolve(data));

                client.sendMessage(
                    {
                        QueueUrl: 'test-queue',
                        MessageAttributes: { MessageAttribute: testMessageAttribute },
                        MessageBody: 'body'.repeat(1024 * 1024),
                    },
                    callback
                );
            });
        } catch (err) {
            result = err;
        }

        // Then
        expect(result.message).toEqual('test error');
    });

    it('should throw S3 error (using promise())', async () => {
        // Given
        const sqs = {
            sendMessage: jest.fn(() => ({})),
        };
        const s3 = {
            putObject: jest.fn(() => ({ promise: () => Promise.reject(new Error('test error')) })),
        };

        const client = new ExtendedSqsClient(sqs, s3, { bucketName: 'test-bucket' });

        // When
        let result;

        try {
            await client
                .sendMessage({
                    QueueUrl: 'test-queue',
                    MessageAttributes: { MessageAttribute: testMessageAttribute },
                    MessageBody: 'body'.repeat(1024 * 1024),
                })
                .promise();
        } catch (err) {
            result = err;
        }

        // Then
        expect(result.message).toEqual('test error');
    });

    it('should throw S3 error (using callback)', async () => {
        // Given
        const sqs = {
            sendMessage: jest.fn(() => ({})),
        };
        const s3 = {
            putObject: jest.fn(() => ({ promise: () => Promise.reject(new Error('test error')) })),
        };

        const client = new ExtendedSqsClient(sqs, s3, { bucketName: 'test-bucket' });

        // When
        let result;

        try {
            await new Promise((resolve, reject) => {
                const callback = (err, data) => (err ? reject(err) : resolve(data));

                client.sendMessage(
                    {
                        QueueUrl: 'test-queue',
                        MessageAttributes: { MessageAttribute: testMessageAttribute },
                        MessageBody: 'body'.repeat(1024 * 1024),
                    },
                    callback
                );
            });
        } catch (err) {
            result = err;
        }

        // Then
        expect(result.message).toEqual('test error');
    });
});

describe('ExtendedSqsClient sendMessageBatch', () => {
    it('should batch send messages', async () => {
        // Given
        const sqs = {
            sendMessageBatch: jest.fn(() => ({ promise: () => Promise.resolve('success') })),
        };
        const s3 = {
            putObject: jest.fn(() => ({ promise: () => Promise.resolve() })),
        };

        const client = new ExtendedSqsClient(sqs, s3, { bucketName: 'test-bucket' });

        const largeMessageBody1 = 'body1'.repeat(1024 * 1024);
        const largeMessageBody2 = 'body2'.repeat(1024 * 1024);

        // When
        await client
            .sendMessageBatch({
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
            })
            .promise();

        // Then
        expect(s3.putObject).toHaveBeenCalledTimes(2);
        expect(s3.putObject.mock.calls[0][0]).toEqual({
            Bucket: 'test-bucket',
            Key: mockS3Key,
            Body: largeMessageBody1,
        });
        expect(s3.putObject.mock.calls[1][0]).toEqual({
            Bucket: 'test-bucket',
            Key: mockS3Key,
            Body: largeMessageBody2,
        });

        expect(sqs.sendMessageBatch).toHaveBeenCalledTimes(1);
        expect(sqs.sendMessageBatch.mock.calls[0][0]).toEqual({
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
});

describe('ExtendedSqsClient receiveMessage', () => {
    it('should receive a message not using S3', async () => {
        // Given
        const messages = {
            Messages: [
                {
                    Body: 'message body',
                    ReceiptHandler: 'receipthandle',
                    MessageAttributes: {
                        MessageAttribute: testMessageAttribute,
                    },
                },
            ],
        };
        const sqs = {
            receiveMessage: jest.fn(() => ({ promise: () => Promise.resolve(messages) })),
        };

        const client = new ExtendedSqsClient(sqs, {});

        // When
        const response = await client
            .receiveMessage({
                QueueUrl: 'test-queue',
                MessageAttributeNames: ['MessageAttribute'],
            })
            .promise();

        // Then
        expect(sqs.receiveMessage).toHaveBeenCalledTimes(1);
        expect(sqs.receiveMessage.mock.calls[0][0]).toEqual({
            MessageAttributeNames: ['MessageAttribute', 'S3MessageBodyKey'],
            QueueUrl: 'test-queue',
        });

        expect(response).toEqual(messages);
    });

    it('should receive a message using S3 (using promise())', async () => {
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
        const sqs = {
            receiveMessage: jest.fn(() => ({ promise: () => Promise.resolve(messages) })),
        };

        const s3Content = {
            Body: Buffer.from('message body'),
        };
        const s3 = {
            getObject: jest.fn(() => ({ promise: () => Promise.resolve(s3Content) })),
        };

        const client = new ExtendedSqsClient(sqs, s3);

        // When
        const response = await client
            .receiveMessage({
                QueueUrl: 'test-queue',
                MessageAttributeNames: ['MessageAttribute'],
            })
            .promise();

        // Then
        expect(sqs.receiveMessage).toHaveBeenCalledTimes(1);
        expect(sqs.receiveMessage.mock.calls[0][0]).toEqual({
            MessageAttributeNames: ['MessageAttribute', 'S3MessageBodyKey'],
            QueueUrl: 'test-queue',
        });

        expect(s3.getObject).toHaveBeenCalledTimes(1);
        expect(s3.getObject.mock.calls[0][0]).toEqual({
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

    it('should receive a message using S3 (using callback)', async () => {
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
        const sqs = {
            receiveMessage: jest.fn(() => ({ promise: () => Promise.resolve(messages) })),
        };

        const s3Content = {
            Body: Buffer.from('message body'),
        };
        const s3 = {
            getObject: jest.fn(() => ({ promise: () => Promise.resolve(s3Content) })),
        };

        const client = new ExtendedSqsClient(sqs, s3);

        // When
        const response = await new Promise((resolve, reject) => {
            const callback = (err, data) => (err ? reject(err) : resolve(data));

            client.receiveMessage(
                {
                    QueueUrl: 'test-queue',
                    MessageAttributeNames: ['MessageAttribute'],
                },
                callback
            );
        });

        // Then
        expect(sqs.receiveMessage).toHaveBeenCalledTimes(1);
        expect(sqs.receiveMessage.mock.calls[0][0]).toEqual({
            MessageAttributeNames: ['MessageAttribute', 'S3MessageBodyKey'],
            QueueUrl: 'test-queue',
        });

        expect(s3.getObject).toHaveBeenCalledTimes(1);
        expect(s3.getObject.mock.calls[0][0]).toEqual({
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

    it('should receive a message using S3 (using send() callback)', async () => {
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
        const sqs = {
            receiveMessage: jest.fn(() => ({ promise: () => Promise.resolve(messages) })),
        };

        const s3Content = {
            Body: Buffer.from('message body'),
        };
        const s3 = {
            getObject: jest.fn(() => ({ promise: () => Promise.resolve(s3Content) })),
        };

        const client = new ExtendedSqsClient(sqs, s3);

        // When
        const response = await new Promise((resolve, reject) => {
            const callback = (err, data) => (err ? reject(err) : resolve(data));

            const request = client.receiveMessage({
                QueueUrl: 'test-queue',
                MessageAttributeNames: ['MessageAttribute'],
            });

            request.send(callback);
        });

        // Then
        expect(sqs.receiveMessage).toHaveBeenCalledTimes(1);
        expect(sqs.receiveMessage.mock.calls[0][0]).toEqual({
            MessageAttributeNames: ['MessageAttribute', 'S3MessageBodyKey'],
            QueueUrl: 'test-queue',
        });

        expect(s3.getObject).toHaveBeenCalledTimes(1);
        expect(s3.getObject.mock.calls[0][0]).toEqual({
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
        const sqs = {
            receiveMessage: jest.fn(() => ({ promise: () => Promise.resolve(messages) })),
        };

        const s3Response = {
            Body: Buffer.from('custom s3 content'),
        };
        const s3 = {
            getObject: jest.fn(() => ({ promise: () => Promise.resolve(s3Response) })),
        };

        const client = new ExtendedSqsClient(sqs, s3, {
            receiveTransform: (message, s3Content) => `${message.Body} - ${s3Content}`,
        });

        // When
        const response = await client
            .receiveMessage({
                QueueUrl: 'test-queue',
            })
            .promise();

        // Then
        expect(sqs.receiveMessage).toHaveBeenCalledTimes(1);
        expect(sqs.receiveMessage.mock.calls[0][0]).toEqual({
            MessageAttributeNames: ['S3MessageBodyKey'],
            QueueUrl: 'test-queue',
        });

        expect(s3.getObject).toHaveBeenCalledTimes(1);
        expect(s3.getObject.mock.calls[0][0]).toEqual({
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

    it('should throw SQS error (using promise())', async () => {
        // Given
        const sqs = {
            receiveMessage: jest.fn(() => ({ promise: () => Promise.reject(new Error('test error')) })),
        };
        const s3 = {
            getObject: jest.fn(() => ({})),
        };

        const client = new ExtendedSqsClient(sqs, s3, { bucketName: 'test-bucket' });

        // When
        let result;

        try {
            await client
                .receiveMessage({
                    QueueUrl: 'test-queue',
                    MessageAttributeNames: ['MessageAttribute'],
                })
                .promise();
        } catch (err) {
            result = err;
        }

        // Then
        expect(result.message).toEqual('test error');
    });

    it('should throw SQS error (using callback)', async () => {
        // Given
        const sqs = {
            receiveMessage: jest.fn(() => ({ promise: () => Promise.reject(new Error('test error')) })),
        };
        const s3 = {
            getObject: jest.fn(() => ({})),
        };

        const client = new ExtendedSqsClient(sqs, s3, { bucketName: 'test-bucket' });

        // When
        let result;

        try {
            await new Promise((resolve, reject) => {
                const callback = (err, data) => (err ? reject(err) : resolve(data));

                client.receiveMessage(
                    {
                        QueueUrl: 'test-queue',
                        MessageAttributeNames: ['MessageAttribute'],
                    },
                    callback
                );
            });
        } catch (err) {
            result = err;
        }

        // Then
        expect(result.message).toEqual('test error');
    });

    it('should throw S3 error (using promise())', async () => {
        // Given
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
        const sqs = {
            receiveMessage: jest.fn(() => ({ promise: () => Promise.resolve(messages) })),
        };

        const s3 = {
            getObject: jest.fn(() => ({ promise: () => Promise.reject(new Error('test error')) })),
        };

        const client = new ExtendedSqsClient(sqs, s3, { bucketName: 'test-bucket' });

        // When
        let result;

        try {
            await client
                .receiveMessage({
                    QueueUrl: 'test-queue',
                    MessageAttributeNames: ['MessageAttribute'],
                })
                .promise();
        } catch (err) {
            result = err;
        }

        // Then
        expect(result.message).toEqual('test error');
    });

    it('should throw S3 error (using callback)', async () => {
        // Given
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
        const sqs = {
            receiveMessage: jest.fn(() => ({ promise: () => Promise.resolve(messages) })),
        };

        const s3 = {
            getObject: jest.fn(() => ({ promise: () => Promise.reject(new Error('test error')) })),
        };

        const client = new ExtendedSqsClient(sqs, s3, { bucketName: 'test-bucket' });

        // When
        let result;

        try {
            await new Promise((resolve, reject) => {
                const callback = (err, data) => (err ? reject(err) : resolve(data));

                client.receiveMessage(
                    {
                        QueueUrl: 'test-queue',
                        MessageAttributeNames: ['MessageAttribute'],
                    },
                    callback
                );
            });
        } catch (err) {
            result = err;
        }

        // Then
        expect(result.message).toEqual('test error');
    });
});

describe('ExtendedSqsClient deleteMessage', () => {
    it('should delete a message not using S3', async () => {
        // Given
        const sqs = {
            deleteMessage: jest.fn(() => ({ promise: () => Promise.resolve('success') })),
        };

        const client = new ExtendedSqsClient(sqs, {}, { bucketName: 'test-bucket' });

        // When
        await client
            .deleteMessage({
                QueueUrl: 'test-queue',
                ReceiptHandle: 'receipthandle',
            })
            .promise();

        // Then
        expect(sqs.deleteMessage).toHaveBeenCalledTimes(1);
        expect(sqs.deleteMessage.mock.calls[0][0]).toEqual({
            QueueUrl: 'test-queue',
            ReceiptHandle: 'receipthandle',
        });
    });

    it('should delete a message and S3 message content', async () => {
        // Given
        const sqs = {
            deleteMessage: jest.fn(() => ({ promise: () => Promise.resolve('success') })),
        };
        const s3 = {
            deleteObject: jest.fn(() => ({ promise: () => Promise.resolve('success') })),
        };

        const client = new ExtendedSqsClient(sqs, s3, { bucketName: 'test-bucket' });

        // When
        await client
            .deleteMessage({
                QueueUrl: 'test-queue',
                ReceiptHandle: `-..s3BucketName..-test-bucket-..s3BucketName..--..s3Key..-${mockS3Key}-..s3Key..-receipthandle`,
            })
            .promise();

        // Then
        expect(sqs.deleteMessage).toHaveBeenCalledTimes(1);
        expect(sqs.deleteMessage.mock.calls[0][0]).toEqual({
            QueueUrl: 'test-queue',
            ReceiptHandle: 'receipthandle',
        });

        expect(s3.deleteObject).toHaveBeenCalledTimes(1);
        expect(s3.deleteObject.mock.calls[0][0]).toEqual({
            Bucket: 'test-bucket',
            Key: mockS3Key,
        });
    });
});

describe('ExtendedSqsClient deleteMessageBatch', () => {
    it('should batch delete messages', async () => {
        // Given
        const sqs = {
            deleteMessageBatch: jest.fn(() => ({ promise: () => Promise.resolve('success') })),
        };
        const s3 = {
            deleteObject: jest.fn(() => ({ promise: () => Promise.resolve('success') })),
        };

        const client = new ExtendedSqsClient(sqs, s3, { bucketName: 'test-bucket' });

        // When
        await client
            .deleteMessageBatch({
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
            })
            .promise();

        // Then
        expect(sqs.deleteMessageBatch).toHaveBeenCalledTimes(1);
        expect(sqs.deleteMessageBatch.mock.calls[0][0]).toEqual({
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

        expect(s3.deleteObject).toHaveBeenCalledTimes(2);
        expect(s3.deleteObject.mock.calls[0][0]).toEqual({
            Bucket: 'test-bucket',
            Key: `${mockS3Key}1`,
        });
        expect(s3.deleteObject.mock.calls[1][0]).toEqual({
            Bucket: 'test-bucket',
            Key: `${mockS3Key}2`,
        });
    });
});

describe('ExtendedSqsClient changeMessageVisibility', () => {
    it('should change visibility of a message not using S3', async () => {
        // Given
        const sqs = {
            changeMessageVisibility: jest.fn(() => ({ promise: () => Promise.resolve('success') })),
        };

        const client = new ExtendedSqsClient(sqs, {}, { bucketName: 'test-bucket' });

        // When
        await client
            .changeMessageVisibility({
                QueueUrl: 'test-queue',
                ReceiptHandle: 'receipthandle',
            })
            .promise();

        // Then
        expect(sqs.changeMessageVisibility).toHaveBeenCalledTimes(1);
        expect(sqs.changeMessageVisibility.mock.calls[0][0]).toEqual({
            QueueUrl: 'test-queue',
            ReceiptHandle: 'receipthandle',
        });
    });

    it('should change visibility of a message using S3', async () => {
        // Given
        const sqs = {
            changeMessageVisibility: jest.fn(() => ({ promise: () => Promise.resolve('success') })),
        };

        const client = new ExtendedSqsClient(sqs, {}, { bucketName: 'test-bucket' });

        // When
        await client
            .changeMessageVisibility({
                QueueUrl: 'test-queue',
                ReceiptHandle: `-..s3BucketName..-test-bucket-..s3BucketName..--..s3Key..-${mockS3Key}-..s3Key..-receipthandle`,
            })
            .promise();

        // Then
        expect(sqs.changeMessageVisibility).toHaveBeenCalledTimes(1);
        expect(sqs.changeMessageVisibility.mock.calls[0][0]).toEqual({
            QueueUrl: 'test-queue',
            ReceiptHandle: 'receipthandle',
        });
    });
});

describe('ExtendedSqsClient changeMessageVisibilityBatch', () => {
    it('should batch change visibility', async () => {
        // Given
        const sqs = {
            changeMessageVisibilityBatch: jest.fn(() => ({ promise: () => Promise.resolve('success') })),
        };

        const client = new ExtendedSqsClient(sqs, {}, { bucketName: 'test-bucket' });

        // When
        await client
            .changeMessageVisibilityBatch({
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
            })
            .promise();

        // Then
        expect(sqs.changeMessageVisibilityBatch).toHaveBeenCalledTimes(1);
        expect(sqs.changeMessageVisibilityBatch.mock.calls[0][0]).toEqual({
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

        const middleware = new ExtendedSqsClient({}, {}, { bucketName: 'test-bucket' }).middleware();

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

        const s3Content = {
            Body: Buffer.from('message body'),
        };
        const s3 = {
            getObject: jest.fn(() => ({ promise: () => Promise.resolve(s3Content) })),
        };

        const middleware = new ExtendedSqsClient({}, s3, { bucketName: 'test-bucket' }).middleware();

        // When
        await middleware.before(handler)

        // Then
        expect(s3.getObject).toHaveBeenCalledTimes(1);
        expect(s3.getObject.mock.calls[0][0]).toEqual({
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

        const s3Response = {
            Body: Buffer.from('custom s3 content'),
        };
        const s3 = {
            getObject: jest.fn(() => ({ promise: () => Promise.resolve(s3Response) })),
        };

        const middleware = new ExtendedSqsClient({}, s3, {
            bucketName: 'test-bucket',
            receiveTransform: (message, s3Content) => `${message.body} - ${s3Content}`,
        }).middleware();

        // When
        await middleware.before(handler);

        // Then
        expect(s3.getObject).toHaveBeenCalledTimes(1);
        expect(s3.getObject.mock.calls[0][0]).toEqual({
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
});
