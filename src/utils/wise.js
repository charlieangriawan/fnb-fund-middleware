import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, BatchWriteCommand } from '@aws-sdk/lib-dynamodb';

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
