export function calcSplit(transaction) {
    if (transaction?.type !== 'DEBIT') return null;
    const participants = {
        jacky: transaction.jacky ?? 0,
        lina: transaction.lina ?? 0,
        charlie: transaction.charlie ?? 0,
        hendro: transaction.hendro ?? 0,
    };
    const total = Object.values(participants).reduce((sum, v) => sum + v, 0);
    if (total === 0) return null;
    const amount = transaction.record.amount;
    const split = Object.fromEntries(
        Object.entries(participants).map(([key, weight]) => [key, (amount / total) * weight])
    );
    return { participants, split };
}
