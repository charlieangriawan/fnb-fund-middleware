const corsHeaders = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type,Authorization',
};

export const reply = (statusCode, body) => ({
    statusCode,
    headers: corsHeaders,
    body: JSON.stringify(body),
});
