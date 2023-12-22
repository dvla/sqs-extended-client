/* eslint-disable no-underscore-dangle */
const { v4: uuidv4 } = require('uuid');
const {
    SQSClient,
    ChangeMessageVisibilityCommand,
    ChangeMessageVisibilityBatchCommand,
    DeleteMessageCommand,
    DeleteMessageBatchCommand,
    SendMessageCommand,
    SendMessageBatchCommand,
    ReceiveMessageCommand,
} = require('@aws-sdk/client-sqs');
const { S3Client, PutObjectCommand, GetObjectCommand, DeleteObjectCommand } = require('@aws-sdk/client-s3');
const { isLarge } = require('./sqsMessageSizeUtils');

const S3_MESSAGE_KEY_MARKER = '-..s3Key..-';
const S3_BUCKET_NAME_MARKER = '-..s3BucketName..-';
const S3_MESSAGE_BODY_KEY = 'S3MessageBodyKey';

function defaultSendTransform(alwaysUseS3, messageSizeThreshold) {
    return (message) => {
        const useS3 = alwaysUseS3 || isLarge(message, messageSizeThreshold);

        return {
            messageBody: useS3 ? null : message.MessageBody,
            s3Content: useS3 ? message.MessageBody : null,
        };
    };
}

function defaultReceiveTransform() {
    return (message, s3Content) => s3Content || message.body || message.Body;
}

function getS3MessageKeyAndBucket(message) {
    const messageAttributes = message.messageAttributes || message.MessageAttributes || {};

    if (!messageAttributes[S3_MESSAGE_BODY_KEY]) {
        return {
            bucketName: null,
            s3MessageKey: null,
        };
    }

    const s3MessageKeyAttr = messageAttributes[S3_MESSAGE_BODY_KEY];
    const s3MessageKey = s3MessageKeyAttr.stringValue || s3MessageKeyAttr.StringValue;

    if (!s3MessageKey) {
        throw new Error(`Invalid ${S3_MESSAGE_BODY_KEY} message attribute: Missing stringValue/StringValue`);
    }

    const s3MessageKeyRegexMatch = s3MessageKey.match(/^\((.*)\)(.+)/);

    return {
        bucketName: s3MessageKeyRegexMatch[1],
        s3MessageKey: s3MessageKeyRegexMatch[2],
    };
}

function embedS3MarkersInReceiptHandle(bucketName, s3MessageKey, receiptHandle) {
    return `${S3_BUCKET_NAME_MARKER}${bucketName}${S3_BUCKET_NAME_MARKER}${S3_MESSAGE_KEY_MARKER}${s3MessageKey}${S3_MESSAGE_KEY_MARKER}${receiptHandle}`;
}

function extractBucketNameFromReceiptHandle(receiptHandle) {
    if (receiptHandle.indexOf(S3_BUCKET_NAME_MARKER) >= 0) {
        return receiptHandle.substring(
            receiptHandle.indexOf(S3_BUCKET_NAME_MARKER) + S3_BUCKET_NAME_MARKER.length,
            receiptHandle.lastIndexOf(S3_BUCKET_NAME_MARKER)
        );
    }

    return null;
}

function extractS3MessageKeyFromReceiptHandle(receiptHandle) {
    if (receiptHandle.indexOf(S3_MESSAGE_KEY_MARKER) >= 0) {
        return receiptHandle.substring(
            receiptHandle.indexOf(S3_MESSAGE_KEY_MARKER) + S3_MESSAGE_KEY_MARKER.length,
            receiptHandle.lastIndexOf(S3_MESSAGE_KEY_MARKER)
        );
    }

    return null;
}

function getOriginReceiptHandle(receiptHandle) {
    return receiptHandle.indexOf(S3_MESSAGE_KEY_MARKER) >= 0
        ? receiptHandle.substring(receiptHandle.lastIndexOf(S3_MESSAGE_KEY_MARKER) + S3_MESSAGE_KEY_MARKER.length)
        : receiptHandle;
}

function addS3MessageKeyAttribute(s3MessageKey, attributes) {
    return {
        ...attributes,
        [S3_MESSAGE_BODY_KEY]: {
            DataType: 'String',
            StringValue: s3MessageKey,
        },
    };
}

class ExtendedSqsClient {
    constructor(options = {}) {
        this.sqsClientConfig = options.sqsClientConfig || {};
        this.s3ClientConfig = options.s3ClientConfig || {};
        this.sqsClient = new SQSClient(this.sqsClientConfig);
        this.s3Client = new S3Client(this.s3ClientConfig);
        this.bucketName = options.bucketName;

        this.sendTransform =
            options.sendTransform || defaultSendTransform(options.alwaysUseS3, options.messageSizeThreshold);
        this.receiveTransform = options.receiveTransform || defaultReceiveTransform();
    }

    _storeS3Content(key, s3Content) {
        const putObjectCommandInput = {
            Bucket: this.bucketName,
            Key: key,
            Body: s3Content,
        };
        const putObjectCommand = new PutObjectCommand(putObjectCommandInput);
        return this.s3Client.send(putObjectCommand);
    }

    async _getS3Content(bucketName, key) {
        const getObjectCommandInput = {
            Bucket: bucketName,
            Key: key,
        };
        const getObjectCommand = new GetObjectCommand(getObjectCommandInput);
        const object = await this.s3Client.send(getObjectCommand);
        return object.Body.transformToString();
    }

    _deleteS3Content(bucketName, key) {
        const deleteObjectCommandInput = {
            Bucket: bucketName,
            Key: key,
        };
        const deleteObjectCommand = new DeleteObjectCommand(deleteObjectCommandInput);
        return this.s3Client.send(deleteObjectCommand);
    }

    middleware() {
        return {
            before: async ({ event }) => {
                await Promise.all(
                    event.Records.map(async (record) => {
                        const { bucketName, s3MessageKey } = getS3MessageKeyAndBucket(record);

                        if (s3MessageKey) {
                            /* eslint-disable-next-line no-param-reassign */
                            record.body = this.receiveTransform(
                                record,
                                await this._getS3Content(bucketName, s3MessageKey)
                            );
                            /* eslint-disable-next-line no-param-reassign */
                            record.receiptHandle = embedS3MarkersInReceiptHandle(
                                bucketName,
                                s3MessageKey,
                                record.receiptHandle
                            );
                        } else {
                            /* eslint-disable-next-line no-param-reassign */
                            record.body = this.receiveTransform(record);
                        }
                    })
                );
            },
        };
    }

    changeMessageVisibility(params) {
        const changeMessageVisibilityCommandInput = {
            ...params,
            ReceiptHandle: getOriginReceiptHandle(params.ReceiptHandle),
        };
        const changeMessageVisibilityCommand = new ChangeMessageVisibilityCommand(changeMessageVisibilityCommandInput);

        return this.sqsClient.send(changeMessageVisibilityCommand);
    }

    changeMessageVisibilityBatch(params) {
        const changeMessageVisibilityBatchCommandInput = {
            ...params,
            Entries: params.Entries.map((entry) => ({
                ...entry,
                ReceiptHandle: getOriginReceiptHandle(entry.ReceiptHandle),
            })),
        };
        const changeMessageVisibilityBatchCommand = new ChangeMessageVisibilityBatchCommand(
            changeMessageVisibilityBatchCommandInput
        );

        return this.sqsClient.send(changeMessageVisibilityBatchCommand);
    }

    /* eslint-disable-next-line class-methods-use-this */
    _prepareDelete(params) {
        return {
            bucketName: extractBucketNameFromReceiptHandle(params.ReceiptHandle),
            s3MessageKey: extractS3MessageKeyFromReceiptHandle(params.ReceiptHandle),
            deleteParams: {
                ...params,
                ReceiptHandle: getOriginReceiptHandle(params.ReceiptHandle),
            },
        };
    }

    async deleteMessage(params) {
        const { bucketName, s3MessageKey, deleteParams } = this._prepareDelete(params);

        const deleteMessageCommand = new DeleteMessageCommand(deleteParams);

        if (!s3MessageKey) {
            return this.sqsClient.send(deleteMessageCommand);
        }

        await this._deleteS3Content(bucketName, s3MessageKey);

        return this.sqsClient.send(deleteMessageCommand);
    }

    async deleteMessageBatch(params) {
        const entryObjs = params.Entries.map((entry) => this._prepareDelete(entry));

        const deleteParams = { ...params };
        deleteParams.Entries = entryObjs.map((entryObj) => entryObj.deleteParams);

        const deleteMessageBatchCommand = new DeleteMessageBatchCommand(deleteParams);

        await Promise.all(
            entryObjs.map(({ bucketName, s3MessageKey }) => {
                if (s3MessageKey) {
                    return this._deleteS3Content(bucketName, s3MessageKey);
                }
                return Promise.resolve();
            })
        );

        return this.sqsClient.send(deleteMessageBatchCommand);
    }

    _prepareSend(params) {
        const sendParams = { ...params };

        const sendObj = this.sendTransform(sendParams);
        const existingS3MessageKey = params.MessageAttributes?.[ExtendedSqsClient.RESERVED_ATTRIBUTE_NAME];
        let s3MessageKey;

        if (!sendObj.s3Content || existingS3MessageKey) {
            s3MessageKey = existingS3MessageKey
            sendParams.MessageBody = sendObj.messageBody || existingS3MessageKey.StringValue;
        } else {
            s3MessageKey = uuidv4();
            sendParams.MessageAttributes = addS3MessageKeyAttribute(
                `(${this.bucketName})${s3MessageKey}`,
                sendParams.MessageAttributes
            );
            sendParams.MessageBody = sendObj.messageBody || s3MessageKey;
        }

        return {
            s3MessageKey,
            sendParams,
            s3Content: sendObj.s3Content,
        };
    }

    async sendMessage(params) {
        if (!this.bucketName) {
            throw new Error('bucketName option is required for sending messages');
        }

        const { s3MessageKey, sendParams, s3Content } = this._prepareSend(params);

        const sendMessageCommand = new SendMessageCommand(sendParams);

        if (!s3MessageKey) {
            return this.sqsClient.send(sendMessageCommand);
        }

        await this._storeS3Content(s3MessageKey, s3Content);

        return this.sqsClient.send(sendMessageCommand);
    }

    async sendMessageBatch(params) {
        if (!this.bucketName) {
            throw new Error('bucketName option is required for sending messages');
        }

        const entryObjs = params.Entries.map((entry) => this._prepareSend(entry));

        const sendParams = { ...params };
        sendParams.Entries = entryObjs.map((entryObj) => entryObj.sendParams);

        const sendMessageBatchCommand = new SendMessageBatchCommand(sendParams);

        await Promise.all(
            entryObjs.map(({ s3Content, s3MessageKey }) => {
                if (s3MessageKey) {
                    return this._storeS3Content(s3MessageKey, s3Content);
                }
                return Promise.resolve();
            })
        );

        return this.sqsClient.send(sendMessageBatchCommand);
    }

    async _processReceive(response) {
        const messages = await Promise.all(
            (response.Messages || []).map(async (input) => {
                const message = { ...input };
                const { bucketName, s3MessageKey } = getS3MessageKeyAndBucket(message);
                if (s3MessageKey) {
                    message.Body = this.receiveTransform(message, await this._getS3Content(bucketName, s3MessageKey));
                    message.ReceiptHandle = embedS3MarkersInReceiptHandle(
                        bucketName,
                        s3MessageKey,
                        message.ReceiptHandle
                    );
                } else {
                    message.Body = this.receiveTransform(message);
                }
                return message;
            })
        );
        return { ...response, Messages: messages.length > 0 ? messages : undefined };
    }

    async receiveMessage(params) {
        const modifiedParams = {
            ...params,
            MessageAttributeNames: [...(params.MessageAttributeNames || []), ExtendedSqsClient.RESERVED_ATTRIBUTE_NAME],
        };
        const receiveMessageCommand = new ReceiveMessageCommand(modifiedParams);
        const response = await this.sqsClient.send(receiveMessageCommand);
        return this._processReceive(response);
    }
}

ExtendedSqsClient.RESERVED_ATTRIBUTE_NAME = S3_MESSAGE_BODY_KEY;

module.exports = ExtendedSqsClient;
