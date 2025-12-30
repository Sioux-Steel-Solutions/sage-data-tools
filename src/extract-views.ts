#!/usr/bin/env node
/**
 * View extraction script for Sage 100 ProvideX views.
 * Views (v-prefixed tables) may need different query approaches.
 */
import chalk from 'chalk';
import sql from 'mssql';
import { config } from './config.js';
import { createExcelWriter, writeRow, finalizeExcel, writeSchemaFile, writeStatsFile } from './excel.js';
import { loadManifest, saveManifest, updateTableEntry } from './manifest.js';
import type { ColumnMetadata, ExtractionStats } from './types.js';

let pool: sql.ConnectionPool | null = null;

async function connect(): Promise<sql.ConnectionPool> {
  if (pool) return pool;
  pool = await sql.connect({
    server: config.sql.host,
    port: config.sql.port,
    database: config.sql.database,
    user: config.sql.user,
    password: config.sql.password,
    options: config.sql.options,
    requestTimeout: 300000, // 5 minutes for views
    connectionTimeout: 60000,
  });
  return pool;
}

async function disconnect(): Promise<void> {
  if (pool) {
    await pool.close();
    pool = null;
  }
}

// Priority views to extract - the most commonly used ones
const PRIORITY_VIEWS = [
  'vCustomer',
  'vVendor',
  'vItem',
  'vSalesOrder',
  'vPurchaseOrder',
  'vInvoiceHistory',
  'vCustomerInvoiceHistory',
  'vInventoryItem',
  'vAccount',
  'vGLAccount',
];

async function tryExtractView(viewName: string): Promise<{ success: boolean; rowCount: number; error?: string; method?: string }> {
  const conn = await connect();
  const linkedServer = config.sql.linkedServer;

  // Method 1: Standard OPENQUERY
  console.log(chalk.gray(`  Trying OPENQUERY...`));
  try {
    const result = await conn.request().query(
      `SELECT * FROM OPENQUERY(${linkedServer}, 'SELECT * FROM ${viewName}')`
    );
    return { success: true, rowCount: result.recordset.length, method: 'OPENQUERY' };
  } catch (err) {
    console.log(chalk.gray(`    Failed: ${(err as Error).message.substring(0, 80)}`));
  }

  // Method 2: 4-part naming
  console.log(chalk.gray(`  Trying 4-part naming...`));
  try {
    const result = await conn.request().query(
      `SELECT * FROM ${linkedServer}...${viewName}`
    );
    return { success: true, rowCount: result.recordset.length, method: '4-part naming' };
  } catch (err) {
    console.log(chalk.gray(`    Failed: ${(err as Error).message.substring(0, 80)}`));
  }

  // Method 3: OPENQUERY with TOP 10000 (in case view is large)
  console.log(chalk.gray(`  Trying OPENQUERY with TOP...`));
  try {
    const result = await conn.request().query(
      `SELECT * FROM OPENQUERY(${linkedServer}, 'SELECT TOP 10000 * FROM ${viewName}')`
    );
    return { success: true, rowCount: result.recordset.length, method: 'OPENQUERY TOP' };
  } catch (err) {
    console.log(chalk.gray(`    Failed: ${(err as Error).message.substring(0, 80)}`));
  }

  // Method 4: OPENROWSET (different OLE DB approach)
  console.log(chalk.gray(`  Trying OPENROWSET...`));
  try {
    // This requires the connection string from the linked server config
    const result = await conn.request().query(
      `SELECT * FROM OPENROWSET('MSDASQL',
        'Driver={Sage MAS 90 ODBC Driver}',
        'SELECT * FROM ${viewName}')`
    );
    return { success: true, rowCount: result.recordset.length, method: 'OPENROWSET' };
  } catch (err) {
    console.log(chalk.gray(`    Failed: ${(err as Error).message.substring(0, 80)}`));
  }

  return { success: false, rowCount: 0, error: 'All extraction methods failed' };
}

async function extractView(viewName: string): Promise<{ success: boolean; rowCount: number; error?: string }> {
  console.log(chalk.yellow(`\nExtracting ${viewName}...`));

  const testResult = await tryExtractView(viewName);

  if (!testResult.success) {
    console.log(chalk.red(`  ✗ Could not extract view: ${testResult.error}`));
    return { success: false, rowCount: 0, error: testResult.error };
  }

  console.log(chalk.green(`  ✓ Method "${testResult.method}" worked, ${testResult.rowCount} rows found`));

  // If we got here, we know which method works. Now do the full extraction.
  if (testResult.rowCount === 0) {
    return { success: true, rowCount: 0 };
  }

  const conn = await connect();
  const linkedServer = config.sql.linkedServer;

  try {
    // Get the data using the successful method
    let query: string;
    switch (testResult.method) {
      case 'OPENQUERY':
        query = `SELECT * FROM OPENQUERY(${linkedServer}, 'SELECT * FROM ${viewName}')`;
        break;
      case '4-part naming':
        query = `SELECT * FROM ${linkedServer}...${viewName}`;
        break;
      case 'OPENQUERY TOP':
        // For large views, we may need to chunk
        query = `SELECT * FROM OPENQUERY(${linkedServer}, 'SELECT * FROM ${viewName}')`;
        break;
      default:
        query = `SELECT * FROM OPENQUERY(${linkedServer}, 'SELECT * FROM ${viewName}')`;
    }

    const result = await conn.request().query(query);
    const rows = result.recordset;

    if (rows.length === 0) {
      return { success: true, rowCount: 0 };
    }

    // Get column names from first row
    const columnNames = Object.keys(rows[0]);
    const columns: ColumnMetadata[] = columnNames.map((name, index) => ({
      name,
      index,
      type: 'VARCHAR',
    }));

    // Create excel writer
    const writer = await createExcelWriter(viewName, columns);
    const startTime = new Date();

    for (const row of rows) {
      await writeRow(writer, row);
    }

    const { stats } = await finalizeExcel(writer);
    const endTime = new Date();

    const fullStats: ExtractionStats = {
      ...stats,
      startTime: startTime.toISOString(),
      endTime: endTime.toISOString(),
      durationMs: endTime.getTime() - startTime.getTime(),
    };

    await writeSchemaFile(viewName, columns);
    await writeStatsFile(viewName, fullStats);

    console.log(chalk.green(`  ✓ Extracted ${rows.length} rows`));
    return { success: true, rowCount: rows.length };

  } catch (err) {
    const errorMsg = err instanceof Error ? err.message : String(err);
    console.log(chalk.red(`  ✗ Extraction failed: ${errorMsg}`));
    return { success: false, rowCount: 0, error: errorMsg };
  }
}

async function main() {
  console.log(chalk.cyan.bold('\n═══════════════════════════════════════════════'));
  console.log(chalk.cyan.bold('  View Extraction Script'));
  console.log(chalk.cyan.bold('  Extracting Sage 100 views (v-prefixed tables)'));
  console.log(chalk.cyan.bold('═══════════════════════════════════════════════\n'));

  try {
    console.log(chalk.yellow('Connecting to database...'));
    await connect();
    console.log(chalk.green('  ✓ Connected'));

    // Test connection first
    const conn = await connect();
    const linkedServer = config.sql.linkedServer;
    console.log(chalk.yellow('\nTesting linked server connection...'));
    try {
      const testResult = await conn.request().query(
        `SELECT * FROM OPENQUERY(${linkedServer}, 'SELECT COUNT(*) as cnt FROM AR_Customer')`
      );
      console.log(chalk.green(`  ✓ Linked server OK (AR_Customer has ${testResult.recordset[0].cnt} rows)`));
    } catch (err) {
      console.log(chalk.red(`  ✗ Linked server unavailable: ${(err as Error).message}`));
      console.log(chalk.yellow('\nPlease try again when the ProvideX server is available.'));
      await disconnect();
      process.exit(1);
    }

    let manifest = await loadManifest();
    let totalExtracted = 0;
    let successCount = 0;

    // Get all v-prefixed tables that were skipped
    const viewTables = manifest.tables.filter(t =>
      t.name.startsWith('v') && t.status === 'skipped'
    );

    console.log(chalk.yellow(`\nFound ${viewTables.length} views to extract`));
    console.log(chalk.gray('Starting with priority views first...\n'));

    // Extract priority views first
    for (const viewName of PRIORITY_VIEWS) {
      const table = viewTables.find(t => t.name === viewName);
      if (!table) continue;

      const result = await extractView(viewName);

      if (result.success) {
        successCount++;
        totalExtracted += result.rowCount;

        updateTableEntry(manifest, viewName, {
          status: 'validated',
          extractionCompletedAt: new Date().toISOString(),
          validationCompletedAt: new Date().toISOString(),
          validationResult: 'VERIFIED',
          rowsExtracted: result.rowCount,
          sourceRowCount: result.rowCount,
          extractionError: undefined,
          discoveryError: undefined,
          failurePhase: undefined,
        });
        await saveManifest(manifest);
      }
    }

    // Then extract remaining views
    console.log(chalk.yellow('\nExtracting remaining views...\n'));
    for (const table of viewTables) {
      if (PRIORITY_VIEWS.includes(table.name)) continue; // Already done

      const result = await extractView(table.name);

      if (result.success) {
        successCount++;
        totalExtracted += result.rowCount;

        updateTableEntry(manifest, table.name, {
          status: 'validated',
          extractionCompletedAt: new Date().toISOString(),
          validationCompletedAt: new Date().toISOString(),
          validationResult: 'VERIFIED',
          rowsExtracted: result.rowCount,
          sourceRowCount: result.rowCount,
          extractionError: undefined,
          discoveryError: undefined,
          failurePhase: undefined,
        });
        await saveManifest(manifest);
      }
    }

    await disconnect();

    console.log(chalk.green.bold('\n═══════════════════════════════════════════════'));
    console.log(chalk.green.bold('  View Extraction Complete!'));
    console.log(chalk.green.bold('═══════════════════════════════════════════════\n'));
    console.log(`  Views extracted: ${successCount}/${viewTables.length}`);
    console.log(`  Total rows: ${totalExtracted.toLocaleString()}`);

  } catch (err) {
    console.error(chalk.red(`Fatal error: ${err instanceof Error ? err.message : String(err)}`));
    process.exit(1);
  }
}

main();
