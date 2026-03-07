import { reply } from '#src/utils/response.js';

export const healthHandler = async (event) => {
    const env = process.env.ENV || 'development';

    return reply(200, { status: 'OK', environment: env });
};
