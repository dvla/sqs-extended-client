/**
 * Example of lambda function which send message with big content to SQS.
 */
const SqsExtendedClient = require('sqs-extended-client');

const DEFAULT_MESSAGE_SIZE_THRESHOLD = 262144;
const QUEUE_URL = '--YOUR-QUEUE-URL--'

const sqsClientConfig = { region: 'eu-west-2'};
const s3ClientConfig = { region: 'eu-west-2'};

const sqsExtendedClient = new SqsExtendedClient({
    sqsClientConfig,
    s3ClientConfig,
    bucketName: '--BUCKET-NAME--',
    messageSizeThreshold: DEFAULT_MESSAGE_SIZE_THRESHOLD
});

const handler = async (event, context ) => {

    const content = '--BIG-CONTENT--';

    const sqsResponse = await sqsExtendedClient.sendMessage({
        QueueUrl: QUEUE_URL,
        MessageBody: JSON.stringify(content),
    });

    return {
        statusCode: 200,
        body: {
            MessageId: sqsResponse.MessageId,
        }
    }
}

module.exports = { handler }