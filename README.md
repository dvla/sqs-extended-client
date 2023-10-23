# SQS Extended Client

A library for managing large AWS SQS message payloads using S3. In particular it supports 
message payloads that exceed the 256KB SQS limit. It is largely a Javascript version 
of the [Amazon SQS Extended Client Library for Java](https://github.com/awslabs/amazon-sqs-java-extended-client-lib), 
although not an exact copy.

This module is intended to be used within node18.x lambda runtime environment, as AWS will remove support for node 16.x in the distant future.

If this module will be used in a node16.x lambda runtime, @aws-sdk dependency have to be bundled into the consumer app.

**Breaking change!**

`SqsExtendedClient()` do not requires passing aws sqs and s3 clients as a parameters anymore.


## Install

To install the SQS Extended Client run:

```
npm install sqs-extended-client
```

## Usage

The SQS Extended Client uses SQS and S3 instances from the AWS SDK v3. 
In order to send messages a `bucketName` is required, which is the S3 bucket where the message payloads will be stored:

```Javascript
const SqsExtendedClient = require('sqs-extended-client');

const sqsExtendedClient = new SqsExtendedClient(
    {
        // required for send message
        bucketName: '/* your bucket name */',
        // other configuration options
        sqsClientConfig: {}, 
        s3ClientConfig: {}
    }
);
```

The SQS Extended Client is used exactly as an SQS instance from the AWS SDK. 
It supports all the message level functions:

```Javascript
changeMessageVisibility()
changeMessageVisibilityBatch()
deleteMessage()
deleteMessageBatch()
sendMessage()
sendMessageBatch()
receiveMessage()

// e.g.
const response = await sqsExtendedClient.receiveMessage({
    QueueUrl: queueUrl,
});
```
For bucket level functions (e.g. createBucket) use the SDK S3 instance directly.

Note that for `sendMessageBatch()` only the size of each message is considered, not the overall batch size. 
For this reason it is recommended to either use `alwaysUseS3: true` or reduce the message size threshold 
proportionally to the maximum batch size (e.g. `messageSizeThreshold: 26214`) when sending batches.

## Options

The SQS Extended Client supports the following options:

* `bucketName` - S3 bucket where message payloads are stored (required for sending messages)
* `sqsClientConfig` - optional SQS client options
* `s3ClientConfig` - optional S3 client options
* `alwaysUseS3` - flag indicating that messages payloads should always be stored in S3 regardless of size (default: `false`)
* `messageSizeThreshold` - maximum size in bytes for message payloads before they are stored in S3 (default: `262144`)
* `sendTransform` - see _Transforms_ section
* `receiveTransform` - see _Transforms_ section

Note that the use of transforms overrides the `alwaysUseS3` and `messageSizeThreshold` options.

## Transforms

The SQS Extended Client allows transforms to be specified that control which elements from the message are stored in S3 and what remains as the SQS message body. By default the whole payload is uploaded to S3 if the message is over the size threshold, the transforms override this behaviour.

There are two transform functions:

* `sendTransform` - Splits a message into an object containing the `messageBody` to send to SQS and the `s3Content` to store in S3.
* `receiveTransform` - Recombines the `message` received from SQS and `s3Content` retrieved from S3 into the full message body.

For example, the following transforms split only the `largeItem` property from a JSON message body to store in S3. The rest of the message body is passed to SQS:

```Javascript
const sendTransform = (sqsMessage) => {
    const { largeItem, ...messageBody } = sqsMessage.MessageBody;
    return {
        s3Content: largeItem,
        messageBody: JSON.stringify(messageBody),
    };
};

const receiveTransform = (sqsMessage, s3Content) => ({
    ...JSON.parse(sqsMessage.Body),
    largeItem: s3Content,
});

const sqsExtendedClient = new SqsExtendedClient(
    {
        bucketName: '/* your bucket name */',
        sendTransform,
        receiveTransform,
    }
);
```

## Middleware

If using [Middy](https://github.com/middyjs/middy) middleware with AWS Lambda then the SQS Extended Client provides 
a middleware implementation:

```Javascript
const middy = require('@middy/core');
const SqsExtendedClient = require('sqs-extended-client');

const handler = middy(/* Lambda event handler */)
    .use(new SqsExtendedClient().middleware());
```

## Test

To execute the unit tests run:

```
npm install
npm run test
```

The integration tests require AWS Localstack to be installed and started. 
The system tests can then be run using:

```
npm run integration-test
```

## Examples

[sender.js](examples/sender.js) - AWS Lambda function which send message.

[receiver.js](examples/receiver.js) - AWS Lambda function which receives messages from sqs.

## License

The MIT License (MIT)

Copyright (c) 2020 DVLA

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
