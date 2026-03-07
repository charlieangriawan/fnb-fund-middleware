const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type,Authorization',
};

export const healthHandler = async (event) => {
    const env = process.env.ENV || 'development';

    return {
        statusCode: 200,
        headers: corsHeaders,
        body: JSON.stringify({
            status: 'OK',
            environment: env,
        }),
    };
};
