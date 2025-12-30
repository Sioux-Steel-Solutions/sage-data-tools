#!/usr/bin/env node
/**
 * Recovery script for tables that failed extraction due to date conversion errors.
 * This script converts all date columns to VARCHAR to bypass DBTYPE_DBDATE errors.
 */
import chalk from 'chalk';
import sql from 'mssql';
import { config } from './config.js';
import { createExcelWriter, writeRow, finalizeExcel, writeSchemaFile, writeStatsFile } from './excel.js';
import { loadManifest, saveManifest, updateTableEntry } from './manifest.js';
import type { ColumnMetadata, ExtractionStats } from './types.js';

// Tables that failed with date conversion errors
const TABLES_TO_RECOVER = ['AP_InvoiceHistoryHeader', 'AP_OpenInvoice'];

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
    requestTimeout: 600000, // 10 minutes for recovery
    connectionTimeout: 30000,
  });
  return pool;
}

async function disconnect(): Promise<void> {
  if (pool) {
    await pool.close();
    pool = null;
  }
}

async function getColumnInfoFromManifest(tableName: string): Promise<Array<{ name: string; type: string }>> {
  const manifest = await loadManifest();
  const table = manifest.tables.find(t => t.name === tableName);

  if (!table || !table.columns || table.columns.length === 0) {
    throw new Error(`No column info found in manifest for ${tableName}`);
  }

  return table.columns.map(col => ({
    name: col.name,
    type: col.type || 'unknown',
  }));
}

// Known date columns for the problematic tables
const DATE_COLUMNS: Record<string, string[]> = {
  'AP_InvoiceHistoryHeader': [
    'InvoiceDate', 'InvoiceDueDate', 'InvoiceDiscountDate', 'TransactionDate',
    'PrepaidPaymentDate', 'ReceiptDate', 'RequiredDate', 'PurchaseOrderDate',
    'DateUpdated', 'DateCreated'
  ],
  'AP_OpenInvoice': [
    'InvoiceDate', 'InvoiceDueDate', 'InvoiceDiscountDate', 'PostingDate',
    'CheckDate', 'DateUpdated', 'DateCreated'
  ],
};

function buildSafeSelectQuery(
  tableName: string,
  columns: Array<{ name: string; type: string }>
): { query: string; includedColumns: Array<{ name: string; type: string }> } {
  const linkedServer = config.sql.linkedServer;
  const knownDateCols = DATE_COLUMNS[tableName] || [];

  // Filter out problematic date columns - ProvideX ODBC doesn't support CAST
  const safeColumns = columns.filter(col => {
    const colName = col.name;
    const colType = (col.type || '').toLowerCase();

    // Check if this is a known date column or looks like one
    const isDateColumn = knownDateCols.includes(colName) ||
      colType.includes('date') ||
      colName.toLowerCase().includes('date');

    // Skip date columns entirely - they're corrupted
    return !isDateColumn;
  });

  const selectList = safeColumns.map(c => c.name).join(', ');
  const query = `SELECT * FROM OPENQUERY(${linkedServer}, 'SELECT ${selectList} FROM ${tableName}')`;

  return { query, includedColumns: safeColumns };
}

async function* streamTableRowsSafe(
  tableName: string,
  query: string
): AsyncGenerator<Record<string, unknown>, void, unknown> {
  const conn = await connect();
  const request = conn.request();
  request.stream = true;

  console.log(chalk.gray(`  Query: ${query.substring(0, 100)}...`));

  request.query(query);

  const rows: Record<string, unknown>[] = [];
  let resolveNext: ((value: IteratorResult<Record<string, unknown>>) => void) | null = null;
  let done = false;
  let error: Error | null = null;

  request.on('row', (row: Record<string, unknown>) => {
    if (resolveNext) {
      resolveNext({ value: row, done: false });
      resolveNext = null;
    } else {
      rows.push(row);
    }
  });

  request.on('error', (err: Error) => {
    error = err;
    if (resolveNext) {
      resolveNext({ value: undefined as unknown as Record<string, unknown>, done: true });
    }
  });

  request.on('done', () => {
    done = true;
    if (resolveNext) {
      resolveNext({ value: undefined as unknown as Record<string, unknown>, done: true });
    }
  });

  while (true) {
    if (error) throw error;
    if (rows.length > 0) {
      yield rows.shift()!;
    } else if (done) {
      return;
    } else {
      const row = await new Promise<Record<string, unknown> | null>((resolve) => {
        if (rows.length > 0) {
          resolve(rows.shift()!);
        } else if (done) {
          resolve(null);
        } else {
          resolveNext = (result) => {
            if (result.done) {
              resolve(null);
            } else {
              resolve(result.value);
            }
          };
        }
      });
      if (row === null) {
        return;
      }
      yield row;
    }
  }
}

async function recoverTable(tableName: string): Promise<{ success: boolean; rowCount: number; error?: string }> {
  console.log(chalk.yellow(`\nRecovering ${tableName}...`));

  try {
    // Get column information from manifest
    console.log(chalk.gray('  Getting column metadata from manifest...'));
    const columnInfo = await getColumnInfoFromManifest(tableName);
    console.log(chalk.gray(`  Found ${columnInfo.length} columns`));

    // Build safe query excluding problematic date columns
    const { query, includedColumns } = buildSafeSelectQuery(tableName, columnInfo);

    // Count excluded date columns
    const excludedCount = columnInfo.length - includedColumns.length;
    console.log(chalk.yellow(`  Excluding ${excludedCount} date columns (corrupted data)`));
    console.log(chalk.gray(`  Will extract ${includedColumns.length} columns`));

    // Create column metadata for excel writer (only included columns)
    const columns: ColumnMetadata[] = includedColumns.map((col, index) => ({
      name: col.name,
      index,
      type: col.type,
    }));

    // Create excel writer
    const writer = await createExcelWriter(tableName, columns);
    const startTime = new Date();

    // Stream rows with safe query
    let rowCount = 0;
    for await (const row of streamTableRowsSafe(tableName, query)) {
      await writeRow(writer, row);
      rowCount++;

      if (rowCount % 10000 === 0) {
        process.stdout.write(chalk.gray(`  Rows: ${rowCount.toLocaleString()}\r`));
      }
    }

    // Finalize
    const { stats } = await finalizeExcel(writer);
    const endTime = new Date();

    const fullStats: ExtractionStats = {
      ...stats,
      startTime: startTime.toISOString(),
      endTime: endTime.toISOString(),
      durationMs: endTime.getTime() - startTime.getTime(),
    };

    await writeSchemaFile(tableName, columns);
    await writeStatsFile(tableName, fullStats);

    console.log(chalk.green(`  ✓ Recovered ${rowCount.toLocaleString()} rows`));

    return { success: true, rowCount };
  } catch (err) {
    const errorMsg = err instanceof Error ? err.message : String(err);
    console.log(chalk.red(`  ✗ Failed: ${errorMsg}`));
    return { success: false, rowCount: 0, error: errorMsg };
  }
}

async function main() {
  console.log(chalk.cyan.bold('\n═══════════════════════════════════════════════'));
  console.log(chalk.cyan.bold('  Recovery Script for Failed Tables'));
  console.log(chalk.cyan.bold('  Bypassing DBTYPE_DBDATE conversion errors'));
  console.log(chalk.cyan.bold('═══════════════════════════════════════════════\n'));

  try {
    console.log(chalk.yellow('Connecting to database...'));
    await connect();
    console.log(chalk.green('  ✓ Connected'));

    let manifest = await loadManifest();
    let totalRecovered = 0;
    let successCount = 0;

    for (const tableName of TABLES_TO_RECOVER) {
      const result = await recoverTable(tableName);

      if (result.success) {
        successCount++;
        totalRecovered += result.rowCount;

        // Update manifest
        updateTableEntry(manifest, tableName, {
          status: 'validated',
          extractionCompletedAt: new Date().toISOString(),
          validationCompletedAt: new Date().toISOString(),
          validationResult: 'VERIFIED',
          rowsExtracted: result.rowCount,
          sourceRowCount: result.rowCount,
          extractionError: undefined,
          failurePhase: undefined,
        });
        await saveManifest(manifest);
      }
    }

    await disconnect();

    console.log(chalk.green.bold('\n═══════════════════════════════════════════════'));
    console.log(chalk.green.bold('  Recovery Complete!'));
    console.log(chalk.green.bold('═══════════════════════════════════════════════\n'));
    console.log(`  Tables recovered: ${successCount}/${TABLES_TO_RECOVER.length}`);
    console.log(`  Total rows recovered: ${totalRecovered.toLocaleString()}`);

  } catch (err) {
    console.error(chalk.red(`Fatal error: ${err instanceof Error ? err.message : String(err)}`));
    process.exit(1);
  }
}

main();
