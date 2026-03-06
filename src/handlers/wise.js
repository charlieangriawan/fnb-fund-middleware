import { saveTransactions, getLatestTransactionDate, getTransactions } from '#src/utils/wise.js';

export const wiseWebhookHandler = async (event) => {
    wiseStatementRefreshHandler();

    console.info(event.body);

    return {
        statusCode: 200,
        body: JSON.stringify({
            success: true,
        }),
    };
};

export const wiseStatementRefreshHandler = async () => {
    const latestDate = await getLatestTransactionDate();
    const start = latestDate
        ? new Date(Date.UTC(latestDate.getUTCFullYear(), latestDate.getUTCMonth() - 1, 1))
        : new Date('2025-01-01T00:00:00.000Z');
    
    const profileId = process.env.WISE_PROFILE_ID;
    const balanceId = process.env.WISE_BALANCE_ID;
    const apiKey = process.env.WISE_API_KEY;
    
    const now = new Date();
    const current = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), 1));
    
    const results = [];
    let cursor = new Date(start);
    while (cursor <= current) {
        const year = cursor.getUTCFullYear();
        const month = cursor.getUTCMonth(); // 0-indexed

        const intervalStart = new Date(Date.UTC(year, month, 1, 0, 0, 0, 0)).toISOString();
        const lastDay = new Date(Date.UTC(year, month + 1, 0, 23, 59, 59, 999));
        const intervalEnd = lastDay.toISOString();

        const params = new URLSearchParams({
            currency: 'SGD',
            type: 'COMPACT',
            statementLocale: 'en',
            intervalStart,
            intervalEnd,
        });
        const url = `https://api.transferwise.com/v1/profiles/${profileId}/balance-statements/${balanceId}/statement.json?${params}`;

        const response = await fetch(url, {
            headers: {
                Authorization: `Bearer ${apiKey}`,
            },
        });

        const data = await response.json();
        results.push({ month: `${year}-${String(month + 1).padStart(2, '0')}`, status: response.status, data });

        cursor = new Date(Date.UTC(year, month + 1, 1));
    }

    const transactions = results.flatMap((r) => r.data?.transactions ?? []);

    if (transactions.length > 0) {
        await saveTransactions(transactions);
    }

    return {
        statusCode: 200,
        body: JSON.stringify(transactions),
    };
};

export const wiseStatementHandler = async (event) => {
    const { type, startDate, endDate } = event.queryStringParameters ?? {};
    const items = await getTransactions({ type, startDate, endDate });

    return {
        statusCode: 200,
        body: JSON.stringify(items),
    };
};

export const wiseBalanceHandler = async () => {
    const profileId = process.env.WISE_PROFILE_ID;
    const balanceId = process.env.WISE_BALANCE_ID;
    const apiKey = process.env.WISE_API_KEY;

    const response = await fetch(
        `https://api.transferwise.com/v4/profiles/${profileId}/balances/${balanceId}`,
        {
            headers: {
                Authorization: `Bearer ${apiKey}`,
            },
        }
    );

    const data = await response.json();

    return {
        statusCode: response.status,
        body: JSON.stringify(data),
    };
};
