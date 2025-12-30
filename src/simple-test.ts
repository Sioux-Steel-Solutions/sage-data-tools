#!/usr/bin/env node
import sql from 'mssql';
import { config } from './config.js';

async function test() {
  console.log('Connecting to SQL Server...');
  const pool = await sql.connect({
    server: config.sql.host,
    port: config.sql.port,
    database: config.sql.database,
    user: config.sql.user,
    password: config.sql.password,
    options: config.sql.options,
    requestTimeout: 300000, // 5 minutes
    connectionTimeout: 60000,
  });

  console.log('Connected. Testing linked server query...');
  console.log('This may take a while if the connection is slow...');

  const linkedServer = config.sql.linkedServer;

  try {
    const start = Date.now();
    const result = await pool.request().query(
      `SELECT * FROM OPENQUERY(${linkedServer}, 'SELECT COUNT(*) as cnt FROM AR_Customer')`
    );
    const elapsed = ((Date.now() - start) / 1000).toFixed(1);
    console.log(`SUCCESS! AR_Customer count: ${result.recordset[0].cnt} (took ${elapsed}s)`);
  } catch (err: unknown) {
    const msg = err instanceof Error ? err.message : String(err);
    console.log(`FAILED: ${msg}`);
  }

  await pool.close();
  console.log('Done.');
}

test();
