import { z } from 'zod';

const envSchema = z.object({
  CLICKHOUSE_HOST: z.string().url(),
  CLICKHOUSE_USER: z.string().default('default'),
  CLICKHOUSE_PASSWORD: z.string().default(''),
  CLICKHOUSE_DATABASE: z.string().default('default'),
  REDIS_URL: z.string().url().optional(),
  RATE_LIMIT_REQUESTS_PER_MINUTE: z.coerce.number().default(60),
  NEXT_PUBLIC_APP_URL: z.string().url().optional(),
  NEXT_PUBLIC_ANALYTICS_ID: z.string().optional(),
});

export const env = envSchema.parse(process.env);

export type Env = z.infer<typeof envSchema>;

