export const wiseWebhookHandler = async (event) => {
    console.info(event.body);

    return {
        statusCode: 200,
        body: JSON.stringify({
            success: true,
        }),
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
