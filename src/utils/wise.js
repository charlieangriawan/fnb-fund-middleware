import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, BatchWriteCommand, QueryCommand } from '@aws-sdk/lib-dynamodb';

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

export async function saveTransactions(transactions) {
    const tableName = process.env.WISE_TRANSACTIONS_TABLE;

    const items = transactions.map((t) => {
        const person = resolvePerson(t);
        const item = {
            referenceNumber: t.referenceNumber,
            date: t.date,
            type: resolveType(t),
            record: t,
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
