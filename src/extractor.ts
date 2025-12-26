import { config } from './config.js';
import { connect, disconnect, enumerateTables, smokeTestTable, streamTableRows, getTableRowCount } from './db.js';
import { loadManifest, saveManifest, updateTableEntry } from './manifest.js';
import { createExcelWriter, writeRow, finalizeExcel, writeSchemaFile, writeStatsFile } from './excel.js';
import type { Manifest, TableEntry, UserDecision, ExtractionStats } from './types.js';

export type PhaseCallback = (table: TableEntry, phase: 'discovery' | 'extraction' | 'validation') => void;
export type FailureCallback = (table: TableEntry) => Promise<UserDecision>;
export type ProgressCallback = (manifest: Manifest) => void;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export async function runExtraction(
  onPhaseChange: PhaseCallback,
  onFailure: FailureCallback,
  onProgress: ProgressCallback
): Promise<Manifest> {
  // Load or create manifest
  let manifest = await loadManifest();

  // Connect to database
  await connect();

  // If manifest is empty, enumerate tables
  if (manifest.tables.length === 0) {
    const tables = await enumerateTables();
    manifest.tables = tables.map((t) => ({
      name: t.name,
      type: t.type,
      status: 'pending' as const,
    }));
    await saveManifest(manifest);
  }

  onProgress(manifest);

  // Process each table
  for (const table of manifest.tables) {
    // Skip already completed tables
    if (['validated', 'skipped'].includes(table.status)) {
      continue;
    }

    let shouldRetry = true;
    while (shouldRetry) {
      shouldRetry = false;

      try {
        // Phase 0: Discovery
        if (['pending', 'failed'].includes(table.status) || !table.columns) {
          onPhaseChange(table, 'discovery');
          updateTableEntry(manifest, table.name, { status: 'discovering' });
          await saveManifest(manifest);

          const discovery = await smokeTestTable(table.name);

          if (!discovery.success) {
            updateTableEntry(manifest, table.name, {
              status: 'failed',
              failurePhase: 'discovery',
              discoveryError: discovery.error,
            });
            await saveManifest(manifest);

            const decision = await onFailure(table);
            updateTableEntry(manifest, table.name, { userDecision: decision });
            await saveManifest(manifest);

            if (decision === 'retry') {
              shouldRetry = true;
              updateTableEntry(manifest, table.name, {
                retryCount: (table.retryCount || 0) + 1,
              });
              continue;
            } else if (decision === 'abort') {
              await disconnect();
              return manifest;
            } else {
              updateTableEntry(manifest, table.name, { status: 'skipped' });
              await saveManifest(manifest);
              continue;
            }
          }

          updateTableEntry(manifest, table.name, {
            status: 'discovered',
            discoveredAt: new Date().toISOString(),
            columns: discovery.columns,
            columnCount: discovery.columns?.length || 0,
          });
          await saveManifest(manifest);
          onProgress(manifest);
        }

        // Phase 1: Extraction
        if (table.status === 'discovered' || (table.status === 'failed' && table.failurePhase === 'extraction')) {
          onPhaseChange(table, 'extraction');
          const startTime = new Date();
          updateTableEntry(manifest, table.name, {
            status: 'extracting',
            extractionStartedAt: startTime.toISOString(),
          });
          await saveManifest(manifest);

          try {
            const columns = table.columns || [];
            const writer = createExcelWriter(table.name, columns);

            let rowCount = 0;
            for await (const row of streamTableRows(table.name)) {
              writeRow(writer, row);
              rowCount++;

              // Update progress every 1000 rows
              if (rowCount % 1000 === 0) {
                updateTableEntry(manifest, table.name, {
                  rowsExtracted: rowCount,
                  sheetsCreated: writer.sheetIndex,
                });
                onProgress(manifest);
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
            await saveManifest(manifest);
            onProgress(manifest);
          } catch (err) {
            updateTableEntry(manifest, table.name, {
              status: 'failed',
              failurePhase: 'extraction',
              extractionError: err instanceof Error ? err.message : String(err),
            });
            await saveManifest(manifest);

            const decision = await onFailure(table);
            updateTableEntry(manifest, table.name, { userDecision: decision });
            await saveManifest(manifest);

            if (decision === 'retry') {
              shouldRetry = true;
              updateTableEntry(manifest, table.name, {
                retryCount: (table.retryCount || 0) + 1,
              });
              continue;
            } else if (decision === 'abort') {
              await disconnect();
              return manifest;
            } else {
              updateTableEntry(manifest, table.name, { status: 'skipped' });
              await saveManifest(manifest);
              continue;
            }
          }
        }

        // Phase 2: Validation
        if (table.status === 'extracted' || (table.status === 'failed' && table.failurePhase === 'validation')) {
          onPhaseChange(table, 'validation');
          updateTableEntry(manifest, table.name, {
            status: 'validating',
            validationStartedAt: new Date().toISOString(),
          });
          await saveManifest(manifest);

          try {
            const sourceCount = await getTableRowCount(table.name);
            const extractedCount = table.rowsExtracted || 0;

            let validationResult: 'VERIFIED' | 'ROW_COUNT_MISMATCH' | 'COLUMN_MISMATCH' = 'VERIFIED';

            if (sourceCount !== extractedCount) {
              validationResult = 'ROW_COUNT_MISMATCH';
            }

            updateTableEntry(manifest, table.name, {
              status: 'validated',
              validationCompletedAt: new Date().toISOString(),
              validationResult,
              sourceRowCount: sourceCount,
            });
            await saveManifest(manifest);
            onProgress(manifest);
          } catch (err) {
            updateTableEntry(manifest, table.name, {
              status: 'failed',
              failurePhase: 'validation',
              validationError: err instanceof Error ? err.message : String(err),
            });
            await saveManifest(manifest);

            const decision = await onFailure(table);
            updateTableEntry(manifest, table.name, { userDecision: decision });
            await saveManifest(manifest);

            if (decision === 'retry') {
              shouldRetry = true;
              updateTableEntry(manifest, table.name, {
                retryCount: (table.retryCount || 0) + 1,
              });
              continue;
            } else if (decision === 'abort') {
              await disconnect();
              return manifest;
            } else {
              updateTableEntry(manifest, table.name, { status: 'skipped' });
              await saveManifest(manifest);
              continue;
            }
          }
        }

        // Sleep between tables
        await sleep(config.execution.sleepBetweenTablesMs);
      } catch (err) {
        // Catch any unexpected errors
        updateTableEntry(manifest, table.name, {
          status: 'failed',
          extractionError: err instanceof Error ? err.message : String(err),
        });
        await saveManifest(manifest);

        const decision = await onFailure(table);
        if (decision === 'abort') {
          await disconnect();
          return manifest;
        } else if (decision === 'retry') {
          shouldRetry = true;
        } else {
          updateTableEntry(manifest, table.name, { status: 'skipped' });
        }
        await saveManifest(manifest);
      }
    }
  }

  await disconnect();
  return manifest;
}
