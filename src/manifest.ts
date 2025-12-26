import { readFile, writeFile } from 'fs/promises';
import { existsSync } from 'fs';
import { config } from './config.js';
import type { Manifest, TableEntry } from './types.js';

export function createEmptyManifest(): Manifest {
  return {
    version: '1.0.0',
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    sourceDatabase: config.sql.database,
    tables: [],
    summary: {
      total: 0,
      pending: 0,
      discovered: 0,
      extracted: 0,
      validated: 0,
      failed: 0,
      skipped: 0,
    },
  };
}

export async function loadManifest(): Promise<Manifest> {
  if (!existsSync(config.paths.manifest)) {
    return createEmptyManifest();
  }

  const content = await readFile(config.paths.manifest, 'utf-8');
  return JSON.parse(content) as Manifest;
}

export async function saveManifest(manifest: Manifest): Promise<void> {
  manifest.updatedAt = new Date().toISOString();
  updateSummary(manifest);
  await writeFile(config.paths.manifest, JSON.stringify(manifest, null, 2));
}

export function updateSummary(manifest: Manifest): void {
  manifest.summary = {
    total: manifest.tables.length,
    pending: manifest.tables.filter((t) => t.status === 'pending').length,
    discovered: manifest.tables.filter((t) =>
      ['discovered', 'extracting', 'extracted', 'validating', 'validated'].includes(t.status)
    ).length,
    extracted: manifest.tables.filter((t) =>
      ['extracted', 'validating', 'validated'].includes(t.status)
    ).length,
    validated: manifest.tables.filter((t) => t.status === 'validated').length,
    failed: manifest.tables.filter((t) => t.status === 'failed').length,
    skipped: manifest.tables.filter((t) => t.status === 'skipped').length,
  };
}

export function updateTableEntry(
  manifest: Manifest,
  tableName: string,
  updates: Partial<TableEntry>
): void {
  const table = manifest.tables.find((t) => t.name === tableName);
  if (table) {
    Object.assign(table, updates);
  }
}

export function getNextPendingTable(manifest: Manifest): TableEntry | undefined {
  return manifest.tables.find((t) => t.status === 'pending');
}

export function getTablesNeedingExtraction(manifest: Manifest): TableEntry[] {
  return manifest.tables.filter((t) => t.status === 'discovered');
}

export function getTablesNeedingValidation(manifest: Manifest): TableEntry[] {
  return manifest.tables.filter((t) => t.status === 'extracted');
}
