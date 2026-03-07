import { saveTransactions, saveInjections, resolveType, resolvePerson, resolvePersonColumns, getLatestTransactionDate, getTransaction, getTransactions, updateStatement } from '#src/utils/wise.js';
import deposits from '#src/injections/deposits.js';
import payments from '#src/injections/payments.js';
import { reply } from '#src/utils/response.js';

export const wiseWebhookHandler = async (event) => {
    wiseStatementRefreshHandler();

    console.info(event.body);

    return reply(200, { success: true });
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

    const mappedTransactions = transactions.map((t) => {
        const type = resolveType(t);
        const person = resolvePerson(t);
        const personCols = resolvePersonColumns(type, person);
        const amount = Math.abs(t.amount.value);
        const participants = Object.fromEntries(Object.entries(personCols).filter(([, v]) => v > 0));
        const total = Object.values(participants).reduce((s, v) => s + v, 0);
        const split = type === 'DEBIT' && total > 0
            ? Object.fromEntries(Object.entries(participants).map(([k, v]) => [k, (amount / total) * v]))
            : undefined;
        return {
            referenceNumber: t.referenceNumber,
            date: t.date,
            type,
            record: {
                date: t.date,
                amount,
                participants,
                ...(split !== undefined && { split, merchant: t.details?.merchant?.name, cardLastFourDigits: t.details?.cardLastFourDigits }),
            },
            ...personCols,
        };
    });

    if (mappedTransactions.length > 0) {
        await saveTransactions(mappedTransactions);
    }

    const depositItems = deposits.map(({ person, record, ...d }) => {
        const personCols = resolvePersonColumns('DEPOSIT', person);
        const amount = Math.abs(record.amount.value);
        const participants = Object.fromEntries(Object.entries(personCols).filter(([, v]) => v > 0));
        return {
            ...d,
            record: { date: record.date, amount, participants },
            ...personCols,
        };
    });
    const paymentItems = payments.map(({ record, ...p }) => {
        const merged = { jacky: 1, lina: 1, charlie: 1, hendro: 1, ...p };
        const personCols = { jacky: merged.jacky, lina: merged.lina, charlie: merged.charlie, hendro: merged.hendro };
        const amount = Math.abs(record.amount.value);
        const participants = Object.fromEntries(Object.entries(personCols).filter(([, v]) => v > 0));
        const total = Object.values(participants).reduce((s, v) => s + v, 0);
        const split = total > 0
            ? Object.fromEntries(Object.entries(participants).map(([k, v]) => [k, (amount / total) * v]))
            : {};
        return {
            ...merged,
            record: { date: record.date, amount, participants, split, merchant: record.details?.merchant?.name, cardLastFourDigits: record.details?.cardLastFourDigits },
        };
    });
    await saveInjections([...depositItems, ...paymentItems]);

    return reply(200, { records: transactions.length });
};

export const wiseTransactionHandler = async (event) => {
    const { referenceNumber } = event.queryStringParameters ?? {};

    if (!referenceNumber) {
        return reply(400, { error: 'referenceNumber is required' });
    }

    const transaction = await getTransaction(referenceNumber);

    if (!transaction) {
        return reply(404, { error: 'Transaction not found' });
    }

    if (transaction.type !== 'DEBIT') {
        return reply(400, { error: 'Transaction is not a DEBIT' });
    }

    return reply(200, { transaction: transaction.record });
};

export const wiseTransactionUpdateHandler = async (event) => {
    const { referenceNumber, jacky, lina, charlie, hendro } = JSON.parse(event.body ?? '{}');

    if (!referenceNumber) {
        return reply(400, { error: 'referenceNumber is required' });
    }

    const existing = await getTransaction(referenceNumber);

    if (!existing) {
        return reply(404, { error: 'Transaction not found' });
    }

    if (existing.type !== 'DEBIT') {
        return reply(400, { error: 'Transaction is not a DEBIT' });
    }

    const updatedCols = {
        jacky: jacky ?? existing.jacky,
        lina: lina ?? existing.lina,
        charlie: charlie ?? existing.charlie,
        hendro: hendro ?? existing.hendro,
    };
    const participants = Object.fromEntries(Object.entries(updatedCols).filter(([, v]) => v > 0));
    const amount = Math.abs(existing.record.amount);
    const total = Object.values(participants).reduce((s, v) => s + v, 0);
    const split = total > 0
        ? Object.fromEntries(Object.entries(participants).map(([k, v]) => [k, (amount / total) * v]))
        : {};

    await updateStatement({ referenceNumber, jacky, lina, charlie, hendro, participants, split });

    const transaction = await getTransaction(referenceNumber);

    return reply(200, { success: true, transaction: transaction.record });
};

export const wiseStatementHandler = async (event) => {
    const { type, startDate = '2026-01-31T16:00:00.000000Z', endDate } = event.queryStringParameters ?? {};
    const items = await getTransactions({ type, startDate, endDate });

    return reply(200, items.map((item) => item.record));
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

    const participants = ['jacky', 'lina', 'charlie', 'hendro'];
    const balances = Object.fromEntries(participants.map((p) => [p, 0]));

    const transactions = await getTransactions();
    for (const tx of transactions) {
        const amount = Math.abs(tx.record?.amount ?? 0);
        if (tx.type === 'DEPOSIT' || tx.type === 'TRANSFER' || tx.type === 'CREDIT') {
            for (const p of participants) {
                if (tx[p]) balances[p] += amount * tx[p];
            }
        } else if (tx.type === 'DEBIT') {
            const total = participants.reduce((sum, p) => sum + (tx[p] ?? 0), 0);
            if (total > 0) {
                for (const p of participants) {
                    balances[p] -= (amount / total) * (tx[p] ?? 0);
                }
            }
        }
    }

    const roundedBalances = Object.fromEntries(
        Object.entries(balances).map(([p, v]) => [p, Math.round(v * 100) / 100])
    );

    return reply(response.status, {
        currency: data.amount.currency,
        amount: data.amount.value,
        balances: roundedBalances,
    });
};
