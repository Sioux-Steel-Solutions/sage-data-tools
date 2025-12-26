import dotenv from 'dotenv';
dotenv.config();

export const config = {
  sql: {
    host: process.env.SAGE_SQL_HOST || 'localhost',
    port: parseInt(process.env.SAGE_SQL_PORT || '1433', 10),
    database: process.env.SAGE_SQL_DATABASE || 'master',
    user: process.env.SAGE_SQL_USER || '',
    password: process.env.SAGE_SQL_PASSWORD || '',
    options: {
      encrypt: process.env.SAGE_SQL_ENCRYPT === 'true',
      trustServerCertificate: process.env.SAGE_SQL_TRUST_SERVER_CERT === 'true',
    },
    linkedServer: process.env.SAGE_LINKED_SERVER || 'SAGE',
  },
  execution: {
    maxConcurrency: 1, // Always 1 - serial only
    sleepBetweenTablesMs: parseInt(process.env.SLEEP_BETWEEN_TABLES_MS || '500', 10),
  },
  xlsx: {
    maxRowsPerSheet: 1_000_000, // Leave buffer below Excel's 1,048,576 limit
  },
  paths: {
    exports: './exports',
    manifest: './manifest.json',
  },
} as const;

export type Config = typeof config;
