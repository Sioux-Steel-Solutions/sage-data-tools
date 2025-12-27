#!/usr/bin/env node
import chalk from 'chalk';
import { config } from './config.js';
import { connect, disconnect, enumerateTables, smokeTestTable, streamTableRows, getTableRowCount } from './db.js';
import { loadManifest, saveManifest, updateTableEntry } from './manifest.js';
import { createExcelWriter, writeRow, finalizeExcel, writeSchemaFile, writeStatsFile } from './excel.js';
import type { Manifest, ExtractionStats } from './types.js';

function log(msg: string) {
  console.log(msg);
}

function logPhase(phase: string, table: string) {
  const colors: Record<string, (s: string) => string> = {
    discovery: chalk.blue,
    extraction: chalk.magenta,
    validation: chalk.cyan,
  };
  const color = colors[phase] || chalk.white;
  log(color(`[${phase.toUpperCase()}] ${table}`));
}

function logSuccess(msg: string) {
  log(chalk.green(`  ✓ ${msg}`));
}

function logError(msg: string) {
  log(chalk.red(`  ✗ ${msg}`));
}

function logInfo(msg: string) {
  log(chalk.gray(`  ${msg}`));
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function main() {
  log(chalk.cyan.bold('\n═══════════════════════════════════════════════'));
  log(chalk.cyan.bold('  Sage / ProvideX Data Extraction Tool'));
  log(chalk.cyan.bold('  Read-only extraction with validation'));
  log(chalk.cyan.bold('═══════════════════════════════════════════════\n'));

  try {
    // Connect
    log(chalk.yellow('Connecting to database...'));
    await connect();
    logSuccess('Connected to SQL Server');

    // Load or create manifest
    let manifest = await loadManifest();

    // Enumerate tables if needed
    if (manifest.tables.length === 0) {
      log(chalk.yellow('\nEnumerating tables from SAGE linked server...'));
      const tables = await enumerateTables();
      manifest.tables = tables.map((t) => ({
        name: t.name,
        type: t.type,
        status: 'pending' as const,
      }));
      await saveManifest(manifest);
      logSuccess(`Found ${tables.length} tables`);
    } else {
      logInfo(`Resuming with ${manifest.tables.length} tables in manifest`);
    }

    const total = manifest.tables.length;
    let processed = 0;

    // Process each table
    for (const table of manifest.tables) {
      // Skip already completed tables
      if (['validated', 'skipped'].includes(table.status)) {
        processed++;
        continue;
      }

      processed++;
      log(chalk.gray(`\n[${processed}/${total}]`));

      try {
        // Phase 0: Discovery
        if (['pending', 'failed'].includes(table.status) || !table.columns) {
          logPhase('discovery', table.name);
          updateTableEntry(manifest, table.name, { status: 'discovering' });

          const discovery = await smokeTestTable(table.name);

          if (!discovery.success) {
            logError(`Discovery failed: ${discovery.error}`);
            updateTableEntry(manifest, table.name, {
              status: 'skipped',
              failurePhase: 'discovery',
              discoveryError: discovery.error,
            });
            await saveManifest(manifest);
            continue;
          }

          updateTableEntry(manifest, table.name, {
            status: 'discovered',
            discoveredAt: new Date().toISOString(),
            columns: discovery.columns,
            columnCount: discovery.columns?.length || 0,
          });
          logSuccess(`Found ${discovery.columns?.length || 0} columns`);
          await saveManifest(manifest);
        }

        // Phase 1: Extraction
        if (table.status === 'discovered' || table.columns) {
          logPhase('extraction', table.name);
          const startTime = new Date();
          updateTableEntry(manifest, table.name, {
            status: 'extracting',
            extractionStartedAt: startTime.toISOString(),
          });

          const columns = manifest.tables.find(t => t.name === table.name)?.columns || [];
          const writer = await createExcelWriter(table.name, columns);

          let rowCount = 0;
          for await (const row of streamTableRows(table.name)) {
            await writeRow(writer, row);
            rowCount++;

            // Progress update every 10000 rows
            if (rowCount % 10000 === 0) {
              process.stdout.write(chalk.gray(`  Rows: ${rowCount.toLocaleString()}\r`));
            }
          }

          const { stats } = await finalizeExcel(writer);
          const endTime = new Date();

          const fullStats: ExtractionStats = {
            ...stats,
            startTime: startTime.toISOString(),
            endTime: endTime.toISOString(),
            durationMs: endTime.getTime() - startTime.getTime(),
          };

          await writeSchemaFile(table.name, columns);
          await writeStatsFile(table.name, fullStats);

          updateTableEntry(manifest, table.name, {
            status: 'extracted',
            extractionCompletedAt: endTime.toISOString(),
            rowsExtracted: stats.rowsWritten,
            sheetsCreated: stats.sheetCount,
            rowsPerSheet: stats.rowsPerSheet,
          });

          logSuccess(`Extracted ${stats.rowsWritten.toLocaleString()} rows` +
            (stats.sheetCount > 1 ? ` across ${stats.sheetCount} sheets` : ''));
          await saveManifest(manifest);
        }

        // Phase 2: Validation
        const currentTable = manifest.tables.find(t => t.name === table.name)!;
        if (currentTable.status === 'extracted') {
          logPhase('validation', table.name);
          updateTableEntry(manifest, table.name, {
            status: 'validating',
            validationStartedAt: new Date().toISOString(),
          });

          const sourceCount = await getTableRowCount(table.name);
          const extractedCount = currentTable.rowsExtracted || 0;

          if (sourceCount === extractedCount) {
            updateTableEntry(manifest, table.name, {
              status: 'validated',
              validationCompletedAt: new Date().toISOString(),
              validationResult: 'VERIFIED',
              sourceRowCount: sourceCount,
            });
            logSuccess(`Verified: ${sourceCount.toLocaleString()} rows match`);
          } else {
            updateTableEntry(manifest, table.name, {
              status: 'validated',
              validationCompletedAt: new Date().toISOString(),
              validationResult: 'ROW_COUNT_MISMATCH',
              sourceRowCount: sourceCount,
            });
            logError(`Row count mismatch: source=${sourceCount}, extracted=${extractedCount}`);
          }
          await saveManifest(manifest);
        }

        // Sleep between tables
        await sleep(config.execution.sleepBetweenTablesMs);

      } catch (err) {
        const errorMsg = err instanceof Error ? err.message : String(err);
        logError(`Failed: ${errorMsg}`);
        updateTableEntry(manifest, table.name, {
          status: 'skipped',
          extractionError: errorMsg,
        });
        await saveManifest(manifest);
      }
    }

    // Final summary
    await disconnect();
    manifest = await loadManifest();

    log(chalk.green.bold('\n═══════════════════════════════════════════════'));
    log(chalk.green.bold('  Extraction Complete!'));
    log(chalk.green.bold('═══════════════════════════════════════════════\n'));

    const validated = manifest.tables.filter(t => t.status === 'validated').length;
    const failed = manifest.tables.filter(t => t.status === 'failed').length;
    const skipped = manifest.tables.filter(t => t.status === 'skipped').length;
    const totalRows = manifest.tables.reduce((acc, t) => acc + (t.rowsExtracted || 0), 0);

    log(`  Total tables:     ${manifest.tables.length}`);
    log(chalk.green(`  Validated:        ${validated}`));
    log(chalk.red(`  Failed:           ${failed}`));
    log(chalk.yellow(`  Skipped:          ${skipped}`));
    log(`  Total rows:       ${totalRows.toLocaleString()}`);
    log(chalk.gray(`\n  Output: ./exports/`));
    log(chalk.gray(`  Manifest: ./manifest.json\n`));

  } catch (err) {
    logError(`Fatal error: ${err instanceof Error ? err.message : String(err)}`);
    process.exit(1);
  }
}

main();
