#!/usr/bin/env node
import sql from 'mssql';
import { config } from './config.js';

async function test() {
  try {
    const pool = await sql.connect({
      server: config.sql.host,
      port: config.sql.port,
      database: config.sql.database,
      user: config.sql.user,
      password: config.sql.password,
      options: config.sql.options,
      requestTimeout: 120000,
      connectionTimeout: 60000,
    });

    console.log('Connected to SQL Server');

    // Test linked server
    const linkedServer = config.sql.linkedServer;
    const result = await pool.request().query(
      `SELECT * FROM OPENQUERY(${linkedServer}, 'SELECT COUNT(*) as cnt FROM AR_Customer')`
    );
    console.log(`AR_Customer count: ${result.recordset[0].cnt}`);

    // Test a view with OPENQUERY
    console.log('\nTesting vCustomer with OPENQUERY...');
    try {
      const viewResult = await pool.request().query(
        `SELECT * FROM OPENQUERY(${linkedServer}, 'SELECT COUNT(*) as cnt FROM vCustomer')`
      );
      console.log(`vCustomer count: ${viewResult.recordset[0].cnt}`);
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      console.log(`vCustomer OPENQUERY error: ${msg}`);
    }

    // Try 4-part naming
    console.log('\nTesting vCustomer with 4-part naming...');
    try {
      const viewResult2 = await pool.request().query(
        `SELECT TOP 1 * FROM ${linkedServer}...vCustomer`
      );
      const cols = Object.keys(viewResult2.recordset[0] || {}).join(', ');
      console.log(`vCustomer via 4-part name - got columns: ${cols}`);
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      console.log(`vCustomer 4-part name error: ${msg}`);
    }

    // Try SELECT TOP 1 with OPENQUERY
    console.log('\nTesting vCustomer SELECT TOP 1 with OPENQUERY...');
    try {
      const viewResult3 = await pool.request().query(
        `SELECT * FROM OPENQUERY(${linkedServer}, 'SELECT TOP 1 * FROM vCustomer')`
      );
      const cols = Object.keys(viewResult3.recordset[0] || {}).join(', ');
      console.log(`vCustomer TOP 1 via OPENQUERY - got columns: ${cols}`);
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      console.log(`vCustomer TOP 1 OPENQUERY error: ${msg}`);
    }

    // Check if view exists
    console.log('\nChecking if vCustomer exists in catalog...');
    try {
      const catalogResult = await pool.request().query(
        `EXEC sp_tables_ex '${linkedServer}', 'vCustomer'`
      );
      console.log(`sp_tables_ex result: ${JSON.stringify(catalogResult.recordset)}`);
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      console.log(`sp_tables_ex error: ${msg}`);
    }

    await pool.close();
  } catch (err: unknown) {
    const msg = err instanceof Error ? err.message : String(err);
    console.error(`Connection error: ${msg}`);
  }
}

test();
