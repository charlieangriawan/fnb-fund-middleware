export const healthHandler = async (event) => {
    const env = process.env.ENV || 'development';

    return {
        statusCode: 200,
        body: JSON.stringify({
            status: 'OK',
            environment: env,
        }),
    };
};
