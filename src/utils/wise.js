import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, BatchWriteCommand, GetCommand, PutCommand, QueryCommand, ScanCommand, UpdateCommand } from '@aws-sdk/lib-dynamodb';
import { S3Client, PutObjectCommand, GetObjectCommand, DeleteObjectCommand } from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';

const client = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(client);
const s3Client = new S3Client({});
const S3_BUCKET = 'fnb-fund';

/**
 * Derives the stored `type` from a transaction:
 * - DEPOSIT and TRANSFER (from details.type) are kept as-is
 * - Everything else falls back to the top-level CREDIT / DEBIT
 */
export function resolveType(transaction) {
    const detailsType = transaction.details?.type;
    if (detailsType === 'DEPOSIT' || detailsType === 'TRANSFER') return detailsType;
    return transaction.type; // CREDIT | DEBIT
}

export function resolvePerson(transaction) {
    const detailsType = transaction.details?.type;
    if (detailsType === 'DEPOSIT') return transaction.details?.senderName ?? null;
    if (detailsType === 'TRANSFER') return transaction.details?.recipient?.name ?? null;
    return null;
}

export function resolvePersonColumns(type, name) {
    if (type === 'DEBIT') return { jacky: 1, lina: 1, charlie: 1, hendro: 1 };
    if (type === 'DEPOSIT' || type === 'TRANSFER') {
        const n = (name ?? '').toLowerCase();
        return {
            jacky: n.includes('jacky') ? 1 : 0,
            lina: n.includes('lina') ? 1 : 0,
            charlie: n.includes('charlie') ? 1 : 0,
            hendro: n.includes('hendro') ? 1 : 0,
        };
    }
    return { jacky: 0, lina: 0, charlie: 0, hendro: 0 };
}

export async function getTransaction(referenceNumber) {
    const tableName = process.env.WISE_TRANSACTIONS_TABLE;
    const response = await docClient.send(
        new GetCommand({
            TableName: tableName,
            Key: { referenceNumber },
        }),
    );
    if (!response.Item) return null;
    return response.Item;
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

export async function saveTransactions(items) {
    const tableName = process.env.WISE_TRANSACTIONS_TABLE;

    const writeRequests = items.map((item) => ({ PutRequest: { Item: item } }));

    // DynamoDB BatchWrite supports max 25 items per call
    const BATCH_SIZE = 25;
    for (let i = 0; i < writeRequests.length; i += BATCH_SIZE) {
        const batch = writeRequests.slice(i, i + BATCH_SIZE);
        await docClient.send(
            new BatchWriteCommand({
                RequestItems: { [tableName]: batch },
            }),
        );
    }
}

export async function updateStatement({ referenceNumber, jacky, lina, charlie, hendro, participants, split }) {
    const tableName = process.env.WISE_TRANSACTIONS_TABLE;

    const fields = { jacky, lina, charlie, hendro };
    const setClauses = [];
    const expressionNames = {};
    const expressionValues = {};

    for (const [key, value] of Object.entries(fields)) {
        if (value !== undefined) {
            setClauses.push(`#${key} = :${key}`);
            expressionNames[`#${key}`] = key;
            expressionValues[`:${key}`] = value;
        }
    }

    if (participants !== undefined) {
        setClauses.push('#record.#participants = :participants');
        expressionNames['#record'] = 'record';
        expressionNames['#participants'] = 'participants';
        expressionValues[':participants'] = participants;
    }

    if (split !== undefined) {
        setClauses.push('#record.#split = :split');
        expressionNames['#record'] = 'record';
        expressionNames['#split'] = 'split';
        expressionValues[':split'] = split;
    }

    if (setClauses.length === 0) return;

    await docClient.send(
        new UpdateCommand({
            TableName: tableName,
            Key: { referenceNumber },
            UpdateExpression: `SET ${setClauses.join(', ')}`,
            ExpressionAttributeNames: expressionNames,
            ExpressionAttributeValues: expressionValues,
        }),
    );
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

export async function uploadToS3(key, body, contentType) {
    await s3Client.send(new PutObjectCommand({
        Bucket: S3_BUCKET,
        Key: key,
        Body: body,
        ContentType: contentType,
    }));
}

export async function generateDownloadUrl(key) {
    const command = new GetObjectCommand({
        Bucket: S3_BUCKET,
        Key: key,
    });
    return getSignedUrl(s3Client, command, { expiresIn: 3600 });
}

export async function deleteFromS3(key) {
    await s3Client.send(new DeleteObjectCommand({ Bucket: S3_BUCKET, Key: key }));
}

export async function addImageKey(referenceNumber, key) {
    const tableName = process.env.WISE_TRANSACTIONS_TABLE;
    await docClient.send(
        new UpdateCommand({
            TableName: tableName,
            Key: { referenceNumber },
            UpdateExpression: 'SET #imageKeys = list_append(if_not_exists(#imageKeys, :empty), :newKey)',
            ExpressionAttributeNames: { '#imageKeys': 'imageKeys' },
            ExpressionAttributeValues: { ':newKey': [key], ':empty': [] },
        }),
    );
}

export async function setImageKeys(referenceNumber, keys) {
    const tableName = process.env.WISE_TRANSACTIONS_TABLE;
    await docClient.send(
        new UpdateCommand({
            TableName: tableName,
            Key: { referenceNumber },
            UpdateExpression: 'SET #imageKeys = :keys',
            ExpressionAttributeNames: { '#imageKeys': 'imageKeys' },
            ExpressionAttributeValues: { ':keys': keys },
        }),
    );
}
