const { isLarge, getMessageAttributesSize, DEFAULT_MESSAGE_SIZE_THRESHOLD } = require('../sqsMessageSizeUtils');

describe('sqsMessageSizeUtils isLarge', () => {
    it('should handle messages without message attributes', async () => {
        // Given
        const smallMessage = {
            MessageBody: 'x'.repeat(DEFAULT_MESSAGE_SIZE_THRESHOLD),
        };
        const largeMessage = {
            MessageBody: 'x'.repeat(DEFAULT_MESSAGE_SIZE_THRESHOLD + 1),
        };

        // When/Then
        expect(isLarge(smallMessage)).toBe(false);
        expect(isLarge(largeMessage)).toBe(true);
    });

    it('should handle messages with message attributes', async () => {
        // Given
        const messageAttributes = {
            Attr: {
                DataType: 'String',
                StringValue: 'Value',
            },
        };

        const smallMessage = {
            MessageBody: 'x'.repeat(DEFAULT_MESSAGE_SIZE_THRESHOLD - getMessageAttributesSize(messageAttributes)),
            MessageAttributes: messageAttributes,
        };
        const largeMessage = {
            MessageBody: 'x'.repeat(DEFAULT_MESSAGE_SIZE_THRESHOLD),
            MessageAttributes: messageAttributes,
        };

        // When/Then
        expect(isLarge(smallMessage)).toBe(false);
        expect(isLarge(largeMessage)).toBe(true);
    });
});

describe('sqsMessageSizeUtils getMessageAttributesSize', () => {
    it('should size String message attribute', async () => {
        // Given
        const messageAttributes = {
            Attr: {
                DataType: 'String',
                StringValue: 'Value',
            },
        };

        // When
        const size = getMessageAttributesSize(messageAttributes);

        // Then
        expect(size).toBe('Attr'.length + 'String'.length + 'Value'.length);
    });

    it('should size Binary message attribute', async () => {
        // Given
        const messageAttributes = {
            Attr: {
                DataType: 'Binary',
                BinaryValue: Buffer.from('Value'),
            },
        };

        // When
        const size = getMessageAttributesSize(messageAttributes);

        // Then
        expect(size).toBe('Attr'.length + 'String'.length + 'Value'.length);
    });

    it('should size multiple message attribute', async () => {
        // Given
        const messageAttributes = {
            Attr1: {
                DataType: 'Binary',
                BinaryValue: Buffer.from('Value1'),
            },
            Attr2: {
                DataType: 'String',
                StringValue: 'Value2',
            },
        };

        // When
        const size = getMessageAttributesSize(messageAttributes);

        // Then
        expect(size).toBe(
            'Attr1'.length + 'Binary'.length + 'Value1'.length + 'Attr2'.length + 'String'.length + 'Value2'.length
        );
    });
});
