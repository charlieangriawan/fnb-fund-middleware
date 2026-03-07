import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, BatchWriteCommand, PutCommand, QueryCommand, ScanCommand } from '@aws-sdk/lib-dynamodb';

const client = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(client);

/**
 * Derives the stored `type` from a transaction:
 * - DEPOSIT and TRANSFER (from details.type) are kept as-is
 * - Everything else falls back to the top-level CREDIT / DEBIT
 */
function resolveType(transaction) {
    const detailsType = transaction.details?.type;
    if (detailsType === 'DEPOSIT' || detailsType === 'TRANSFER') return detailsType;
    return transaction.type; // CREDIT | DEBIT
}

function resolvePerson(transaction) {
    const detailsType = transaction.details?.type;
    if (detailsType === 'DEPOSIT') return transaction.details?.senderName ?? null;
    if (detailsType === 'TRANSFER') return transaction.details?.recipient?.name ?? null;
    return null;
}

export async function getLatestTransactionDate() {
    const tableName = process.env.WISE_TRANSACTIONS_TABLE;
    const types = ['CREDIT', 'DEBIT', 'DEPOSIT', 'TRANSFER'];

    let latestDate = null;

    for (const type of types) {
        const response = await docClient.send(
            new QueryCommand({
                TableName: tableName,
                IndexName: 'type-date-index',
                KeyConditionExpression: '#type = :type',
                ExpressionAttributeNames: { '#type': 'type', '#date': 'date' },
                ExpressionAttributeValues: { ':type': type },
                ScanIndexForward: false,
                Limit: 1,
                ProjectionExpression: '#date',
            }),
        );
        const date = response.Items?.[0]?.date;
        if (date && (!latestDate || date > latestDate)) {
            latestDate = date;
        }
    }

    return latestDate ? new Date(latestDate) : null;
}

export async function getTransactions({ type, startDate, endDate } = {}) {
    const tableName = process.env.WISE_TRANSACTIONS_TABLE;
    const types = type ? [type] : ['CREDIT', 'DEBIT', 'DEPOSIT', 'TRANSFER'];

    const allItems = [];

    for (const t of types) {
        let keyCondition = '#type = :type';
        const expressionNames = { '#type': 'type' };
        const expressionValues = { ':type': t };

        if (startDate && endDate) {
            expressionNames['#date'] = 'date';
            keyCondition += ' AND #date BETWEEN :start AND :end';
            expressionValues[':start'] = startDate;
            expressionValues[':end'] = endDate;
        } else if (startDate) {
            expressionNames['#date'] = 'date';
            keyCondition += ' AND #date >= :start';
            expressionValues[':start'] = startDate;
        } else if (endDate) {
            expressionNames['#date'] = 'date';
            keyCondition += ' AND #date <= :end';
            expressionValues[':end'] = endDate;
        }

        let lastKey;
        do {
            const response = await docClient.send(
                new QueryCommand({
                    TableName: tableName,
                    IndexName: 'type-date-index',
                    KeyConditionExpression: keyCondition,
                    ExpressionAttributeNames: expressionNames,
                    ExpressionAttributeValues: expressionValues,
                    ScanIndexForward: false,
                    ...(lastKey && { ExclusiveStartKey: lastKey }),
                }),
            );
            allItems.push(...(response.Items ?? []));
            lastKey = response.LastEvaluatedKey;
        } while (lastKey);
    }

    allItems.sort((a, b) => (a.date < b.date ? 1 : -1));

    return allItems;
}

export async function saveTransactions(transactions) {
    const tableName = process.env.WISE_TRANSACTIONS_TABLE;

    const items = transactions.map((t) => {
        const person = resolvePerson(t);
        const type = resolveType(t);
        const isDebit = type === 'DEBIT';
        const item = {
            referenceNumber: t.referenceNumber,
            date: t.date,
            type,
            record: t,
            jacky: isDebit ? 1 : 0,
            lina: isDebit ? 1 : 0,
            charlie: isDebit ? 1 : 0,
            hendro: isDebit ? 1 : 0,
        };
        if (person !== null) item.person = person;
        return { PutRequest: { Item: item } };
    });

    // DynamoDB BatchWrite supports max 25 items per call
    const BATCH_SIZE = 25;
    for (let i = 0; i < items.length; i += BATCH_SIZE) {
        const batch = items.slice(i, i + BATCH_SIZE);
        await docClient.send(
            new BatchWriteCommand({
                RequestItems: { [tableName]: batch },
            }),
        );
    }
}

export async function saveInjections(items) {
    const tableName = process.env.WISE_TRANSACTIONS_TABLE;

    await Promise.all(
        items.map(async (item) => {
            try {
                await docClient.send(
                    new PutCommand({
                        TableName: tableName,
                        Item: item,
                        ConditionExpression: 'attribute_not_exists(referenceNumber)',
                    }),
                );
            } catch (err) {
                if (err.name !== 'ConditionalCheckFailedException') throw err;
            }
        }),
    );
}
