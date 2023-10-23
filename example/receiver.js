/**
 * Example of lambda function which receives messages from sqs with big content.
 */
const middy = require('@middy/core');
const SqsExtendedClient = require('sqs-extended-client');

const sqsClientConfig = { region: 'eu-west-2'};
const s3ClientConfig = { region: 'eu-west-2'};

const sqsExtendedClient = new SqsExtendedClient({
    sqsClientConfig,
    s3ClientConfig,
    bucketName: '--BUCKET-NAME--'
});

const processRecord = async (data) => {
    // Do whatever is required with every record from sqs.
}

const processEvent = async (event) => {
    await Promise.all(
        event.Records.map(async (record) => {
            return processRecord(record);
        })
    );
};

const handler = middy(processEvent);

// Ensure that middleware is called
handler.use(sqsExtendedClient.middleware());

module.exports = { handler };
