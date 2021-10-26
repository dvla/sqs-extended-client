/* eslint-disable no-underscore-dangle */
const { v4: uuidv4 } = require('uuid');
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
    return (message, s3Content) => {
        return s3Content || message.body || message.Body;
    };
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

    const s3MessageKeyRegexMatch = s3MessageKey.match(/^\((.*)\)(.*)?/);

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

function wrapRequest(request, callback, sendFn) {
    if (callback) {
        sendFn(callback);
    }

    return {
        ...request,
        send: sendFn,
        promise: sendFn,
    };
}

function invokeFnBeforeRequest(request, fn) {
    return (callback) =>
        new Promise((resolve, reject) => {
            fn()
                .then(() => {
                    request
                        .promise()
                        .then((response) => {
                            if (callback) {
                                callback(undefined, response);
                            }

                            resolve(response);
                        })
                        .catch((err) => {
                            if (callback) {
                                callback(err);
                                resolve();
                                return;
                            }

                            reject(err);
                        });
                })
                .catch((fnErr) => {
                    if (callback) {
                        callback(fnErr);
                        resolve();
                        return;
                    }

                    reject(fnErr);
                });
        });
}

function invokeFnAfterRequest(request, fn) {
    return (callback) =>
        new Promise((resolve, reject) => {
            request
                .promise()
                .then((response) => {
                    fn(response)
                        .then(() => {
                            if (callback) {
                                callback(undefined, response);
                            }

                            resolve(response);
                        })
                        .catch((s3Err) => {
                            if (callback) {
                                callback(s3Err);
                                resolve();
                                return;
                            }

                            reject(s3Err);
                        });
                })
                .catch((err) => {
                    if (callback) {
                        callback(err);
                        resolve();
                        return;
                    }

                    reject(err);
                });
        });
}

class ExtendedSqsClient {
    constructor(sqs, s3, options = {}) {
        this.sqs = sqs;
        this.s3 = s3;
        this.bucketName = options.bucketName;

        this.sendTransform =
            options.sendTransform || defaultSendTransform(options.alwaysUseS3, options.messageSizeThreshold);
        this.receiveTransform = options.receiveTransform || defaultReceiveTransform();
    }

    _storeS3Content(key, s3Content) {
        const params = {
            Bucket: this.bucketName,
            Key: key,
            Body: s3Content,
        };

        return this.s3.putObject(params).promise();
    }

    async _getS3Content(bucketName, key) {
        const params = {
            Bucket: bucketName,
            Key: key,
        };

        const object = await this.s3.getObject(params).promise();
        return object.Body.toString();
    }

    _deleteS3Content(bucketName, key) {
        const params = {
            Bucket: bucketName,
            Key: key,
        };

        return this.s3.deleteObject(params).promise();
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
                )
            },
        };
    }

    changeMessageVisibility(params, callback) {
        return this.sqs.changeMessageVisibility(
            {
                ...params,
                ReceiptHandle: getOriginReceiptHandle(params.ReceiptHandle),
            },
            callback
        );
    }

    changeMessageVisibilityBatch(params, callback) {
        return this.sqs.changeMessageVisibilityBatch(
            {
                ...params,
                Entries: params.Entries.map((entry) => ({
                    ...entry,
                    ReceiptHandle: getOriginReceiptHandle(entry.ReceiptHandle),
                })),
            },
            callback
        );
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

    deleteMessage(params, callback) {
        const { bucketName, s3MessageKey, deleteParams } = this._prepareDelete(params);

        if (!s3MessageKey) {
            return this.sqs.deleteMessage(deleteParams, callback);
        }

        const request = this.sqs.deleteMessage(deleteParams);

        return wrapRequest(
            request,
            callback,
            invokeFnBeforeRequest(request, () => this._deleteS3Content(bucketName, s3MessageKey))
        );
    }

    deleteMessageBatch(params, callback) {
        const entryObjs = params.Entries.map((entry) => this._prepareDelete(entry));

        const deleteParams = { ...params };
        deleteParams.Entries = entryObjs.map((entryObj) => entryObj.deleteParams);

        const request = this.sqs.deleteMessageBatch(deleteParams);

        return wrapRequest(
            request,
            callback,
            invokeFnBeforeRequest(request, () =>
                Promise.all(
                    entryObjs.map(({ bucketName, s3MessageKey }) => {
                        if (s3MessageKey) {
                            return this._deleteS3Content(bucketName, s3MessageKey);
                        }
                        return Promise.resolve();
                    })
                )
            )
        );
    }

    _prepareSend(params) {
        const sendParams = { ...params };

        const sendObj = this.sendTransform(sendParams);
        const existingS3MessageKey =
            params.MessageAttributes && params.MessageAttributes[ExtendedSqsClient.RESERVED_ATTRIBUTE_NAME];
        let s3MessageKey;

        if (!sendObj.s3Content || existingS3MessageKey) {
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

    sendMessage(params, callback) {
        if (!this.bucketName) {
            throw new Error('bucketName option is required for sending messages');
        }

        const { s3MessageKey, sendParams, s3Content } = this._prepareSend(params);

        if (!s3MessageKey) {
            return this.sqs.sendMessage(sendParams, callback);
        }

        const request = this.sqs.sendMessage(sendParams);

        return wrapRequest(
            request,
            callback,
            invokeFnBeforeRequest(request, () => this._storeS3Content(s3MessageKey, s3Content))
        );
    }

    sendMessageBatch(params, callback) {
        if (!this.bucketName) {
            throw new Error('bucketName option is required for sending messages');
        }

        const entryObjs = params.Entries.map((entry) => this._prepareSend(entry));

        const sendParams = { ...params };
        sendParams.Entries = entryObjs.map((entryObj) => entryObj.sendParams);

        const request = this.sqs.sendMessageBatch(sendParams);

        return wrapRequest(
            request,
            callback,
            invokeFnBeforeRequest(request, () =>
                Promise.all(
                    entryObjs.map(({ s3Content, s3MessageKey }) => {
                        if (s3MessageKey) {
                            return this._storeS3Content(s3MessageKey, s3Content);
                        }

                        return Promise.resolve();
                    })
                )
            )
        );
    }

    _processReceive() {
        return (response) =>
            Promise.all(
                (response.Messages || []).map(async (message) => {
                    const { bucketName, s3MessageKey } = getS3MessageKeyAndBucket(message);

                    if (s3MessageKey) {
                        /* eslint-disable-next-line no-param-reassign */
                        message.Body = this.receiveTransform(
                            message,
                            await this._getS3Content(bucketName, s3MessageKey)
                        );
                        /* eslint-disable-next-line no-param-reassign */
                        message.ReceiptHandle = embedS3MarkersInReceiptHandle(
                            bucketName,
                            s3MessageKey,
                            message.ReceiptHandle
                        );
                    } else {
                        /* eslint-disable-next-line no-param-reassign */
                        message.Body = this.receiveTransform(message);
                    }
                })
            );
    }

    receiveMessage(params, callback) {
        const modifiedParams = {
            ...params,
            MessageAttributeNames: [...(params.MessageAttributeNames || []), ExtendedSqsClient.RESERVED_ATTRIBUTE_NAME],
        };

        const request = this.sqs.receiveMessage(modifiedParams);
        return wrapRequest(request, callback, invokeFnAfterRequest(request, this._processReceive()));
    }
}

ExtendedSqsClient.RESERVED_ATTRIBUTE_NAME = S3_MESSAGE_BODY_KEY;

module.exports = ExtendedSqsClient;
