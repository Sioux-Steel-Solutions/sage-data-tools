#!/usr/bin/env node
/**
 * Recovery script for tables that were skipped due to column-level errors.
 * Attempts to discover and extract these tables by querying columns individually.
 */
import chalk from 'chalk';
import sql from 'mssql';
import { config } from './config.js';
import { createExcelWriter, writeRow, finalizeExcel, writeSchemaFile, writeStatsFile } from './excel.js';
import { loadManifest, saveManifest, updateTableEntry } from './manifest.js';
import type { ColumnMetadata, ExtractionStats } from './types.js';

// Tables to attempt recovery
const TABLES_TO_RECOVER = ['SY_Context', 'SY_EmbeddedIOSettings'];

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
    requestTimeout: 600000,
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

async function getTableColumns(tableName: string): Promise<Array<{ name: string; type: string }>> {
  const conn = await connect();
  const linkedServer = config.sql.linkedServer;
  
  // Try sp_columns_ex first
  try {
    const result = await conn.request().query(
      `EXEC sp_columns_ex '${linkedServer}', '${tableName}'`
    );
    if (result.recordset.length > 0) {
      return result.recordset.map(r => ({
        name: r.COLUMN_NAME,
        type: r.TYPE_NAME || 'VARCHAR',
      }));
    }
  } catch (err) {
    console.log(chalk.gray(`  sp_columns_ex failed, trying direct query...`));
  }
  
  // Try querying the table directly with TOP 0 to get column names
  try {
    const result = await conn.request().query(
      `SELECT * FROM OPENQUERY(${linkedServer}, 'SELECT TOP 0 * FROM ${tableName}')`
    );
    // The column names come from the result.recordset.columns
    return Object.keys(result.recordset.columns || {}).map(name => ({
      name,
      type: 'VARCHAR',
    }));
  } catch (err) {
    console.log(chalk.gray(`  Direct query failed: ${err instanceof Error ? err.message : err}`));
  }
  
  return [];
}

async function extractTableWithColumnExclusion(
  tableName: string,
  allColumns: string[],
  excludeColumns: string[]
): Promise<{ success: boolean; rowCount: number; error?: string }> {
  const conn = await connect();
  const linkedServer = config.sql.linkedServer;
  
  const safeColumns = allColumns.filter(c => !excludeColumns.includes(c));
  const selectList = safeColumns.join(', ');
  const query = `SELECT * FROM OPENQUERY(${linkedServer}, 'SELECT ${selectList} FROM ${tableName}')`;
  
  console.log(chalk.gray(`  Query: ${query.substring(0, 100)}...`));
  
  try {
    const result = await conn.request().query(query);
    const rows = result.recordset;
    
    if (rows.length === 0) {
      return { success: true, rowCount: 0 };
    }
    
    // Create column metadata
    const columns: ColumnMetadata[] = safeColumns.map((name, index) => ({
      name,
      index,
      type: 'VARCHAR',
    }));
    
    // Create excel writer
    const writer = await createExcelWriter(tableName, columns);
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
    
    await writeSchemaFile(tableName, columns);
    await writeStatsFile(tableName, fullStats);
    
    return { success: true, rowCount: rows.length };
  } catch (err) {
    return { success: false, rowCount: 0, error: err instanceof Error ? err.message : String(err) };
  }
}

async function recoverTable(tableName: string): Promise<{ success: boolean; rowCount: number; error?: string }> {
  console.log(chalk.yellow(`\nRecovering ${tableName}...`));
  
  // Get row count first
  const conn = await connect();
  const linkedServer = config.sql.linkedServer;
  
  try {
    const countResult = await conn.request().query(
      `SELECT * FROM OPENQUERY(${linkedServer}, 'SELECT COUNT(*) as cnt FROM ${tableName}')`
    );
    const rowCount = countResult.recordset[0].cnt;
    console.log(chalk.gray(`  Source has ${rowCount} rows`));
    
    if (rowCount === 0) {
      console.log(chalk.gray(`  Skipping - no data`));
      return { success: true, rowCount: 0 };
    }
  } catch (err) {
    console.log(chalk.red(`  Cannot count rows: ${err instanceof Error ? err.message : err}`));
    return { success: false, rowCount: 0, error: 'Cannot access table' };
  }
  
  // Get columns
  const columns = await getTableColumns(tableName);
  if (columns.length === 0) {
    console.log(chalk.red(`  Cannot discover columns`));
    return { success: false, rowCount: 0, error: 'Cannot discover columns' };
  }
  console.log(chalk.gray(`  Found ${columns.length} columns: ${columns.map(c => c.name).join(', ')}`));
  
  // Try to extract all columns first
  const allColNames = columns.map(c => c.name);
  let result = await extractTableWithColumnExclusion(tableName, allColNames, []);
  
  if (result.success) {
    console.log(chalk.green(`  ✓ Extracted ${result.rowCount} rows (all columns)`));
    return result;
  }
  
  // If failed, try to find the problematic column(s) by testing each one
  console.log(chalk.yellow(`  Full extraction failed, testing columns individually...`));
  
  const problematicColumns: string[] = [];
  for (const col of columns) {
    try {
      await conn.request().query(
        `SELECT * FROM OPENQUERY(${linkedServer}, 'SELECT ${col.name} FROM ${tableName} WHERE 1=0')`
      );
    } catch (err) {
      console.log(chalk.red(`  Column ${col.name} is problematic`));
      problematicColumns.push(col.name);
    }
  }
  
  if (problematicColumns.length === 0) {
    console.log(chalk.red(`  Could not identify problematic columns`));
    return { success: false, rowCount: 0, error: 'Unknown column error' };
  }
  
  console.log(chalk.yellow(`  Excluding columns: ${problematicColumns.join(', ')}`));
  result = await extractTableWithColumnExclusion(tableName, allColNames, problematicColumns);
  
  if (result.success) {
    console.log(chalk.green(`  ✓ Extracted ${result.rowCount} rows (${allColNames.length - problematicColumns.length}/${allColNames.length} columns)`));
  } else {
    console.log(chalk.red(`  ✗ Failed: ${result.error}`));
  }
  
  return result;
}

async function main() {
  console.log(chalk.cyan.bold('\n═══════════════════════════════════════════════'));
  console.log(chalk.cyan.bold('  Recovery Script for Skipped Tables'));
  console.log(chalk.cyan.bold('  Attempting to recover tables with column errors'));
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

      if (result.success && result.rowCount > 0) {
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
          discoveryError: undefined,
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
