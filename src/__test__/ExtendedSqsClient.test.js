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

afterEach(() => {
  jest.resetAllMocks();
})

describe('ExtendedSqsClient sendMessage', () => {
    it.concurrent.each([
        {  sendMessage: jest.fn(() => ({ promise: () => Promise.resolve('success') })) },
        {  sendMessage: jest.fn(() => Promise.resolve('success')) }
    ])('should send small message directly - %#', async (sqs) => {
        // Given
        const client = new ExtendedSqsClient(sqs, {}, { bucketName: 'test-bucket' });

        // When
        const result = client.sendMessage({
            QueueUrl: 'test-queue',
            MessageAttributes: { MessageAttribute: testMessageAttribute },
            MessageBody: 'small message body',
        })
        'promise' in result ? await result.promise() : await result;

        // Then
        expect(sqs.sendMessage).toHaveBeenCalledTimes(1);
        expect(sqs.sendMessage.mock.calls[0][0]).toEqual({
            QueueUrl: 'test-queue',
            MessageBody: 'small message body',
            MessageAttributes: { MessageAttribute: testMessageAttribute },
        });
    });

    it.concurrent.each([
        [
            {  sendMessage: jest.fn(() => ({ promise: () => Promise.resolve({ messageId: 'test message id' }) })) },
            {  putObject: jest.fn(() => ({ promise: () => Promise.resolve() })) }
        ],
        [
            {  sendMessage: jest.fn(() => Promise.resolve({ messageId: 'test message id' })) },
            {  putObject: jest.fn(() => Promise.resolve()) }
        ]
    ])('should use S3 to send large message (using promise()) - %#', async (sqs, s3) => {
        const client = new ExtendedSqsClient(sqs, s3, { bucketName: 'test-bucket' });
        const largeMessageBody = 'body'.repeat(1024 * 1024);

        // When
        const result = client.sendMessage({
            QueueUrl: 'test-queue',
            MessageAttributes: { MessageAttribute: testMessageAttribute },
            MessageBody: largeMessageBody,
        })
        const actual = 'promise' in result ? await result.promise() : await result;

        // Then
        expect(actual).toEqual({ messageId: 'test message id' });
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

    it.concurrent.each([
        [
            {  sendMessage: jest.fn(() => ({ promise: () => Promise.resolve({ messageId: 'test message id' }) })) },
            {  putObject: jest.fn(() => ({ promise: () => Promise.resolve() })) },
        ],
        [
            {  sendMessage: jest.fn(() => Promise.resolve({ messageId: 'test message id' })) },
            {  putObject: jest.fn(() => Promise.resolve()) },
        ]
    ])('should use S3 to send large message (using callback) - %#', async (sqs, s3) => {
        // Given
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
        expect(result).toEqual({ messageId: 'test message id' });
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

    it('should use S3 to send large message (using send() callback) - only compatible with aws-sdk v2', async () => {
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

    it.concurrent.each([
        [
            {  sendMessage: jest.fn(() => ({ promise: () => Promise.resolve('success') })) },
            {  putObject: jest.fn(() => ({ promise: () => Promise.resolve() })) },
        ],
        [
            {  sendMessage: jest.fn(() => Promise.resolve('success')) },
            {  putObject: jest.fn(() => Promise.resolve()) },
        ]
    ])('should use s3 to send small message when alwaysUseS3=true - %#', async (sqs, s3) => {
        // Given
        const client = new ExtendedSqsClient(sqs, s3, {
            bucketName: 'test-bucket',
            alwaysUseS3: true,
        });

        // When
        const result = client.sendMessage({
            QueueUrl: 'test-queue',
            MessageAttributes: { MessageAttribute: testMessageAttribute },
            MessageBody: 'small message body',
        })
        'promise' in result ? await result.promise() : await result;

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

    it.concurrent.each([
        [
            {  sendMessage: jest.fn(() => ({ promise: () => Promise.resolve('success') })) },
            {  putObject: jest.fn(() => ({ promise: () => Promise.resolve() })) },
        ],
        [
            {  sendMessage: jest.fn(() => Promise.resolve('success')) },
            {  putObject: jest.fn(() => Promise.resolve()) },
        ]
    ])('should apply custom sendTransform function - %#', async (sqs, s3) => {
        // Given
        const client = new ExtendedSqsClient(sqs, s3, {
            bucketName: 'test-bucket',
            sendTransform: (message) => ({
                messageBody: `custom ${message.MessageBody}`,
                s3Content: 'custom s3 content',
            }),
        });

        // When
        const result = client.sendMessage({
            QueueUrl: 'test-queue',
            MessageBody: 'message body',
        })
        'promise' in result ? await result.promise() : await result;

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

    it.concurrent.each([
        [
            {  sendMessage: jest.fn(() => ({ promise: () => Promise.resolve('success') })) },
            {  putObject: jest.fn(() => ({ promise: () => Promise.resolve() })) },
        ],
        [
            {  sendMessage: jest.fn(() => Promise.resolve('success')) },
            {  putObject: jest.fn(() => Promise.resolve()) },
        ]
    ])('should respect existing S3MessageBodyKey - %#', async (sqs, s3) => {
        // Given
        const client = new ExtendedSqsClient(sqs, s3, {
            bucketName: 'test-bucket',
            alwaysUseS3: true,
        });

        // When
        const result = client.sendMessage({
            QueueUrl: 'test-queue',
            MessageAttributes: { S3MessageBodyKey: s3MessageBodyKeyAttribute },
            MessageBody: 'message body',
        })
        'promise' in result ? await result.promise() : await result;

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

    it.concurrent.each([
        [
            {  sendMessage: jest.fn(() => ({ promise: () => Promise.reject(new Error('test error')) })) },
            {  putObject: jest.fn(() => ({ promise: () => Promise.resolve() })) },
        ],
        [
            {  sendMessage: jest.fn(() => Promise.reject(new Error('test error'))) },
            {  putObject: jest.fn(() => Promise.resolve()) },
        ]
    ])('should throw SQS error (using promise())', async (sqs, s3) => {
        // Given
        const client = new ExtendedSqsClient(sqs, s3, { bucketName: 'test-bucket' });

        // When
        let result;

        try {
            const result = client.sendMessage({
                QueueUrl: 'test-queue',
                MessageAttributes: { MessageAttribute: testMessageAttribute },
                MessageBody: 'body'.repeat(1024 * 1024),
            })
            'promise' in result ? await result.promise() : await result;
        } catch (err) {
            result = err;
        }

        // Then
        expect(result.message).toEqual('test error');
    });

    it.concurrent.each([
        [
            {  sendMessage: jest.fn(() => ({ promise: () => Promise.reject(new Error('test error')) })) },
            {  putObject: jest.fn(() => ({ promise: () => Promise.resolve() })) },
        ],
        [
            {  sendMessage: jest.fn(() => Promise.reject(new Error('test error'))) },
            {  putObject: jest.fn(() => Promise.resolve()) },
        ]
    ])('should throw SQS error (using callback) - #i', async (sqs, s3) => {
        // Given
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

    it.concurrent.each([
        [
            {  sendMessage: jest.fn(() => ({ promise: () => Promise.resolve({}) })) },
            {  putObject: jest.fn(() => ({ promise: () => Promise.reject(new Error('test error')) })) },
        ],
        [
            {  sendMessage: jest.fn(() => Promise.resolve({})) },
            {  putObject: jest.fn(() => Promise.reject(new Error('test error'))) },
        ]
    ])('should throw S3 error (using promise()) - %#', async (sqs, s3) => {
        // Given
        const client = new ExtendedSqsClient(sqs, s3, { bucketName: 'test-bucket' });

        // When
        let result;

        try {
            const result = client.sendMessage({
                QueueUrl: 'test-queue',
                MessageAttributes: { MessageAttribute: testMessageAttribute },
                MessageBody: 'body'.repeat(1024 * 1024),
            })
            'promise' in result ? await result.promise() : await result;
        } catch (err) {
            result = err;
        }

        // Then
        expect(result.message).toEqual('test error');
    });

    it.concurrent.each([
        [
            {  sendMessage: jest.fn(() => ({ promise: () => Promise.resolve({}) })) },
            {  putObject: jest.fn(() => ({ promise: () => Promise.reject(new Error('test error')) })) },
        ],
        [
            {  sendMessage: jest.fn(() => Promise.resolve({})) },
            {  putObject: jest.fn(() => Promise.reject(new Error('test error'))) },
        ]
    ])('should throw S3 error (using callback) - %#', async (sqs, s3) => {
        // Given
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
    it.concurrent.each([
        [
            {  sendMessageBatch: jest.fn(() => ({ promise: () => Promise.resolve('success') })) },
            {  putObject: jest.fn(() => ({ promise: () => Promise.resolve() })) },
        ],
        [
            {  sendMessageBatch: jest.fn(() => Promise.resolve('success')) },
            {  putObject: jest.fn(() => Promise.resolve()) },
        ]
    ])('should batch send messages - %#', async (sqs, s3) => {
        // Given
        const client = new ExtendedSqsClient(sqs, s3, { bucketName: 'test-bucket' });

        const largeMessageBody1 = 'body1'.repeat(1024 * 1024);
        const largeMessageBody2 = 'body2'.repeat(1024 * 1024);

        // When
        const result = client.sendMessageBatch({
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
        'promise' in result ? await result.promise() : await result;

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
    it.concurrent.each([
        [
            (messages) => ({  receiveMessage: jest.fn(() => ({ promise: () => Promise.resolve(messages) })) }),
        ],
        [
            (messages) => ({  receiveMessage: jest.fn(() => Promise.resolve(messages)) }),
        ]
    ])('should receive a message not using S3 - %#', async (sqsMocker) => {
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
        const sqs = sqsMocker(messages);
        const client = new ExtendedSqsClient(sqs, {});

        // When
        const result = client.receiveMessage({
            QueueUrl: 'test-queue',
            MessageAttributeNames: ['MessageAttribute'],
        })
        const actual = 'promise' in result ? await result.promise() : await result;

        // Then
        expect(sqs.receiveMessage).toHaveBeenCalledTimes(1);
        expect(sqs.receiveMessage.mock.calls[0][0]).toEqual({
            MessageAttributeNames: ['MessageAttribute', 'S3MessageBodyKey'],
            QueueUrl: 'test-queue',
        });

        expect(actual).toEqual(messages);
    });

    it.concurrent.each([
        [
            (messages) => ({  receiveMessage: jest.fn(() => ({ promise: () => Promise.resolve(messages) })) }),
            (content) => ({  getObject: jest.fn(() => ({ promise: () => Promise.resolve(content) })) }),
            { Body: Buffer.from('message body') }
        ],
        [
            (messages) => ({  receiveMessage: jest.fn(() => Promise.resolve(messages)) }),
            (content) => ({  getObject: jest.fn(() => Promise.resolve(content)) }),
            { Body: { transformToString: jest.fn(async () => 'message body') }}
        ]
    ])('should receive a message using S3 (using promise()) - %#', async (sqsMocker, s3Mocker, s3Content) => {
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
        const sqs = sqsMocker(messages);
        const s3 = s3Mocker(s3Content);

        const client = new ExtendedSqsClient(sqs, s3);

        // When
        const result = await client.receiveMessage({
            QueueUrl: 'test-queue',
            MessageAttributeNames: ['MessageAttribute'],
        })
        const actual = 'promise' in result ? await result.promise() : await result;

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
        if ('transformToString' in s3Content.Body) {
            expect(s3Content.Body.transformToString).toHaveBeenCalledTimes(1);
        }

        expect(actual).toEqual({
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

    it.concurrent.each([
        [
            (messages) => ({  receiveMessage: jest.fn(() => ({ promise: () => Promise.resolve(messages) })) }),
            (content) => ({  getObject: jest.fn(() => ({ promise: () => Promise.resolve(content) })) }),
            { Body: Buffer.from('message body') }
        ],
        [
            (messages) => ({  receiveMessage: jest.fn(() => Promise.resolve(messages)) }),
            (content) => ({  getObject: jest.fn(() => Promise.resolve(content)) }),
            { Body: { transformToString: jest.fn(async () => 'message body') }}
        ]
    ])('should receive a message using S3 (using callback) - %#', async (sqsMocker, s3Mocker, s3Content) => {
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
        const sqs = sqsMocker(messages);
        const s3 = s3Mocker(s3Content);

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
        if ('transformToString' in s3Content.Body) {
            expect(s3Content.Body.transformToString).toHaveBeenCalledTimes(1);
        }

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

    it('should receive a message using S3 (using send() callback) - only compatible with aws-sdk v2', async () => {
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

    it.concurrent.each([
        [
            (messages) => ({  receiveMessage: jest.fn(() => ({ promise: () => Promise.resolve(messages) })) }),
            (content) => ({  getObject: jest.fn(() => ({ promise: () => Promise.resolve(content) })) }),
            { Body: Buffer.from('custom s3 content') }
        ],
        [
            (messages) => ({  receiveMessage: jest.fn(() => Promise.resolve(messages)) }),
            (content) => ({  getObject: jest.fn(() => Promise.resolve(content)) }),
            { Body: { transformToString: jest.fn(async () => 'custom s3 content') }}
        ]
    ])('should apply custom receiveTransform function - %#', async (sqsMocker, s3Mocker, s3Response) => {
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
        const sqs = sqsMocker(messages);
        const s3 = s3Mocker(s3Response);

        const client = new ExtendedSqsClient(sqs, s3, {
            receiveTransform: (message, s3Content) => `${message.Body} - ${s3Content}`,
        });

        // When
        const result = client.receiveMessage({
            QueueUrl: 'test-queue',
        })
        const actual = 'promise' in result ? await result.promise() : await result;

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

        expect(actual).toEqual({
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

    it.concurrent.each([
        [
            { receiveMessage: jest.fn(() => ({ promise: () => Promise.reject(new Error('test error')) })) },
            { getObject: jest.fn(() => ({ promise: () => Promise.resolve({}) })) },
        ],
        [
            { receiveMessage: jest.fn(() => Promise.reject(new Error('test error'))) },
            { getObject: jest.fn(() => Promise.resolve({})) },
        ]
    ])('should throw SQS error (using promise()) = %#', async (sqs, s3) => {
        // Given
        const client = new ExtendedSqsClient(sqs, s3, { bucketName: 'test-bucket' });

        // When
        let result;

        try {
            const result = client.receiveMessage({
                QueueUrl: 'test-queue',
                MessageAttributeNames: ['MessageAttribute'],
            })
            'promise' in result ? await result.promise() : await result;
        } catch (err) {
            result = err;
        }

        // Then
        expect(result.message).toEqual('test error');
    });

    it.concurrent.each([
        [
            { receiveMessage: jest.fn(() => ({ promise: () => Promise.reject(new Error('test error')) })) },
            { getObject: jest.fn(() => ({ promise: () => Promise.resolve({}) })) },
        ],
        [
            { receiveMessage: jest.fn(() => Promise.reject(new Error('test error'))) },
            { getObject: jest.fn(() => Promise.resolve({})) },
        ]
    ])('should throw SQS error (using callback) - %#', async (sqs, s3) => {
        // Given
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

    it.concurrent.each([
        [
            (messages) => ({  receiveMessage: jest.fn(() => ({ promise: () => Promise.resolve(messages) })) }),
            { getObject: jest.fn(() => ({ promise: () => Promise.reject(new Error('test error')) })) },
        ],
        [
            (messages) => ({  receiveMessage: jest.fn(() => Promise.resolve(messages)) }),
            { getObject: jest.fn(() => Promise.reject(new Error('test error'))) },
        ]
    ])('should throw S3 error (using promise()) - %#', async (sqsMocker, s3) => {
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
        const sqs = sqsMocker(messages);
        const client = new ExtendedSqsClient(sqs, s3, { bucketName: 'test-bucket' });

        // When
        let result;

        try {
            const result = client.receiveMessage({
                QueueUrl: 'test-queue',
                MessageAttributeNames: ['MessageAttribute'],
            })
            'promise' in result ? await result.promise() : await result;
        } catch (err) {
            result = err;
        }

        // Then
        expect(result.message).toEqual('test error');
    });

    it.concurrent.each([
        [
            (messages) => ({  receiveMessage: jest.fn(() => ({ promise: () => Promise.resolve(messages) })) }),
            { getObject: jest.fn(() => ({ promise: () => Promise.reject(new Error('test error')) })) },
        ],
        [
            (messages) => ({  receiveMessage: jest.fn(() => Promise.resolve(messages)) }),
            { getObject: jest.fn(() => Promise.reject(new Error('test error'))) },
        ]
    ])('should throw S3 error (using callback) - %#', async (sqsMocker, s3) => {
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
        const sqs = sqsMocker(messages);
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
    it.concurrent.each([
        [
            {  deleteMessage: jest.fn(() => ({ promise: () => Promise.resolve('success') })) },
        ],
        [
            {  deleteMessage: jest.fn(() => Promise.resolve('success')) },
        ]
    ])('should delete a message not using S3 - %#', async (sqs) => {
        // Given
        const client = new ExtendedSqsClient(sqs, {}, { bucketName: 'test-bucket' });

        // When
        const result = client.deleteMessage({
            QueueUrl: 'test-queue',
            ReceiptHandle: 'receipthandle',
        })
        'promise' in result ? await result.promise() : await result;

        // Then
        expect(sqs.deleteMessage).toHaveBeenCalledTimes(1);
        expect(sqs.deleteMessage.mock.calls[0][0]).toEqual({
            QueueUrl: 'test-queue',
            ReceiptHandle: 'receipthandle',
        });
    });

    it.concurrent.each([
        [
            {  deleteMessage: jest.fn(() => ({ promise: () => Promise.resolve('success') })) },
            {  deleteObject: jest.fn(() => ({ promise: () => Promise.resolve('success') })) },
        ],
        [
            {  deleteMessage: jest.fn(() => Promise.resolve('success')) },
            {  deleteObject: jest.fn(() => Promise.resolve('success')) },
        ]
    ])('should delete a message and S3 message content - %#', async (sqs, s3) => {
        // Given
        const client = new ExtendedSqsClient(sqs, s3, { bucketName: 'test-bucket' });

        // When
        const result = client.deleteMessage({
            QueueUrl: 'test-queue',
            ReceiptHandle: `-..s3BucketName..-test-bucket-..s3BucketName..--..s3Key..-${mockS3Key}-..s3Key..-receipthandle`,
        })
        'promise' in result ? await result.promise() : await result;

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
    it.concurrent.each([
        [
            {  deleteMessageBatch: jest.fn(() => ({ promise: () => Promise.resolve('success') })) },
            {  deleteObject: jest.fn(() => ({ promise: () => Promise.resolve('success') })) },
        ],
        [
            {  deleteMessageBatch: jest.fn(() => Promise.resolve('success')) },
            {  deleteObject: jest.fn(() => Promise.resolve('success')) },
        ]
    ])('should batch delete messages - %#', async (sqs, s3) => {
        // Given
        const client = new ExtendedSqsClient(sqs, s3, { bucketName: 'test-bucket' });

        // When
        const result = client.deleteMessageBatch({
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
        'promise' in result ? await result.promise() : await result;

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
    it.concurrent.each([
        [
            {  changeMessageVisibility: jest.fn(() => ({ promise: () => Promise.resolve('success') })) },
        ],
        [
            {  changeMessageVisibility: jest.fn(() => Promise.resolve('success')) },
        ]
    ])('should change visibility of a message not using S3 - %#', async (sqs) => {
        // Given
        const client = new ExtendedSqsClient(sqs, {}, { bucketName: 'test-bucket' });

        // When
        const result = client.changeMessageVisibility({
            QueueUrl: 'test-queue',
            ReceiptHandle: 'receipthandle',
        })
        'promise' in result ? await result.promise() : await result;

        // Then
        expect(sqs.changeMessageVisibility).toHaveBeenCalledTimes(1);
        expect(sqs.changeMessageVisibility.mock.calls[0][0]).toEqual({
            QueueUrl: 'test-queue',
            ReceiptHandle: 'receipthandle',
        });
    });

    it.concurrent.each([
        [
            {  changeMessageVisibility: jest.fn(() => ({ promise: () => Promise.resolve('success') })) },
        ],
        [
            {  changeMessageVisibility: jest.fn(() => Promise.resolve('success')) },
        ]
    ])('should change visibility of a message using S3', async (sqs) => {
        // Given
        const client = new ExtendedSqsClient(sqs, {}, { bucketName: 'test-bucket' });

        // When
        const result = client.changeMessageVisibility({
            QueueUrl: 'test-queue',
            ReceiptHandle: `-..s3BucketName..-test-bucket-..s3BucketName..--..s3Key..-${mockS3Key}-..s3Key..-receipthandle`,
        })
        'promise' in result ? await result.promise() : await result;

        // Then
        expect(sqs.changeMessageVisibility).toHaveBeenCalledTimes(1);
        expect(sqs.changeMessageVisibility.mock.calls[0][0]).toEqual({
            QueueUrl: 'test-queue',
            ReceiptHandle: 'receipthandle',
        });
    });
});

describe('ExtendedSqsClient changeMessageVisibilityBatch', () => {
    it.concurrent.each([
        [
            {  changeMessageVisibilityBatch: jest.fn(() => ({ promise: () => Promise.resolve('success') })) },
        ],
        [
            {  changeMessageVisibilityBatch: jest.fn(() => Promise.resolve('success')) },
        ]
    ])('should batch change visibility - %#', async (sqs) => {
        // Given
        const client = new ExtendedSqsClient(sqs, {}, { bucketName: 'test-bucket' });

        // When
        const result = client.changeMessageVisibilityBatch({
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
        'promise' in result ? await result.promise() : await result;

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

    it.concurrent.each([
        [
            (content) => ({  getObject: jest.fn(() => ({ promise: () => Promise.resolve(content) })) }),
            { Body: Buffer.from('message body') }
        ],
        [
            (content) => ({  getObject: jest.fn(() => Promise.resolve(content)) }),
            { Body: { transformToString: jest.fn(async () => 'message body') }}
        ]
    ])('should handle a message using S3 - %#', async (s3Mocker, s3Content) => {
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
        const s3 = s3Mocker(s3Content);
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

    it.concurrent.each([
        [
            (content) => ({  getObject: jest.fn(() => ({ promise: () => Promise.resolve(content) })) }),
            { Body: Buffer.from('custom s3 content') }
        ],
        [
            (content) => ({  getObject: jest.fn(() => Promise.resolve(content)) }),
            { Body: { transformToString: jest.fn(async () => 'custom s3 content') }}
        ]
    ])('should apply custom receiveTransform function', async (s3Mocker, s3Response) => {
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
        const s3 = s3Mocker(s3Response);

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
