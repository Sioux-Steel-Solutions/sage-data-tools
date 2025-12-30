#!/usr/bin/env node
/**
 * Verification script to compare source row counts against extracted counts.
 * Identifies any tables where we may have missed data.
 */
import chalk from 'chalk';
import sql from 'mssql';
import { config } from './config.js';
import { loadManifest } from './manifest.js';

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
    requestTimeout: 120000,
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

async function getSourceRowCount(tableName: string): Promise<number | null> {
  try {
    const conn = await connect();
    const linkedServer = config.sql.linkedServer;
    const query = `SELECT * FROM OPENQUERY(${linkedServer}, 'SELECT COUNT(*) as cnt FROM ${tableName}')`;
    const result = await conn.request().query(query);
    return result.recordset[0].cnt;
  } catch (err) {
    return null; // Table not accessible
  }
}

async function main() {
  console.log(chalk.cyan.bold('\n═══════════════════════════════════════════════'));
  console.log(chalk.cyan.bold('  Row Count Verification'));
  console.log(chalk.cyan.bold('  Comparing source vs extracted counts'));
  console.log(chalk.cyan.bold('═══════════════════════════════════════════════\n'));

  try {
    console.log(chalk.yellow('Connecting to database...'));
    await connect();
    console.log(chalk.green('  ✓ Connected\n'));

    const manifest = await loadManifest();

    // Focus on tables that were validated (successfully extracted)
    const validatedTables = manifest.tables.filter(t => t.status === 'validated');

    console.log(`Checking ${validatedTables.length} validated tables...\n`);

    const mismatches: Array<{
      name: string;
      source: number;
      extracted: number;
      diff: number;
    }> = [];

    const inaccessible: string[] = [];
    let checked = 0;
    let totalSourceRows = 0;
    let totalExtractedRows = 0;

    for (const table of validatedTables) {
      checked++;
      process.stdout.write(chalk.gray(`  [${checked}/${validatedTables.length}] ${table.name}...`));

      const sourceCount = await getSourceRowCount(table.name);
      const extractedCount = table.rowsExtracted || 0;

      if (sourceCount === null) {
        inaccessible.push(table.name);
        process.stdout.write(chalk.yellow(' inaccessible\n'));
        continue;
      }

      totalSourceRows += sourceCount;
      totalExtractedRows += extractedCount;

      if (sourceCount !== extractedCount) {
        mismatches.push({
          name: table.name,
          source: sourceCount,
          extracted: extractedCount,
          diff: sourceCount - extractedCount,
        });
        process.stdout.write(chalk.red(` MISMATCH: source=${sourceCount}, extracted=${extractedCount}\n`));
      } else {
        process.stdout.write(chalk.green(` ✓ ${sourceCount.toLocaleString()} rows\n`));
      }
    }

    // Also check skipped tables for any that might have data
    console.log(chalk.yellow('\nChecking skipped tables for recoverable data...\n'));

    const skippedTables = manifest.tables.filter(t => t.status === 'skipped');
    const skippedWithData: Array<{ name: string; count: number; error?: string }> = [];

    for (const table of skippedTables) {
      process.stdout.write(chalk.gray(`  ${table.name}...`));

      const sourceCount = await getSourceRowCount(table.name);

      if (sourceCount === null) {
        process.stdout.write(chalk.gray(' inaccessible\n'));
      } else if (sourceCount > 0) {
        skippedWithData.push({
          name: table.name,
          count: sourceCount,
          error: table.discoveryError || table.extractionError,
        });
        process.stdout.write(chalk.yellow(` HAS DATA: ${sourceCount.toLocaleString()} rows\n`));
      } else {
        process.stdout.write(chalk.gray(' 0 rows\n'));
      }
    }

    await disconnect();

    // Summary
    console.log(chalk.cyan.bold('\n═══════════════════════════════════════════════'));
    console.log(chalk.cyan.bold('  Verification Summary'));
    console.log(chalk.cyan.bold('═══════════════════════════════════════════════\n'));

    console.log('Validated Tables:');
    console.log(`  Checked: ${checked}`);
    console.log(`  Source rows total: ${totalSourceRows.toLocaleString()}`);
    console.log(`  Extracted rows total: ${totalExtractedRows.toLocaleString()}`);
    console.log(`  Difference: ${(totalSourceRows - totalExtractedRows).toLocaleString()}`);
    console.log('');

    if (mismatches.length > 0) {
      console.log(chalk.red(`Row Count Mismatches (${mismatches.length} tables):`));
      mismatches.forEach(m => {
        console.log(`  ${m.name}: source=${m.source.toLocaleString()}, extracted=${m.extracted.toLocaleString()}, missing=${m.diff.toLocaleString()}`);
      });
      console.log('');
    } else {
      console.log(chalk.green('  No row count mismatches found!\n'));
    }

    if (skippedWithData.length > 0) {
      console.log(chalk.yellow(`Skipped Tables WITH Data (${skippedWithData.length} tables):`));
      skippedWithData.forEach(t => {
        console.log(`  ${t.name}: ${t.count.toLocaleString()} rows`);
        if (t.error) {
          console.log(chalk.gray(`    Error: ${t.error.substring(0, 80)}...`));
        }
      });
      console.log('');
    } else {
      console.log(chalk.green('  No skipped tables with recoverable data!\n'));
    }

    if (inaccessible.length > 0) {
      console.log(chalk.gray(`Inaccessible tables during verification: ${inaccessible.length}`));
    }

  } catch (err) {
    console.error(chalk.red(`Fatal error: ${err instanceof Error ? err.message : String(err)}`));
    process.exit(1);
  }
}

main();
