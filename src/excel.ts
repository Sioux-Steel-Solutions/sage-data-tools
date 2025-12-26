import ExcelJS from 'exceljs';
import { mkdir } from 'fs/promises';
import { existsSync } from 'fs';
import path from 'path';
import { config } from './config.js';
import type { ColumnMetadata, ExtractionStats, SchemaInfo } from './types.js';
import { writeFile } from 'fs/promises';

export interface ExcelWriterState {
  workbook: ExcelJS.Workbook;
  currentSheet: ExcelJS.Worksheet | null;
  tableName: string;
  sheetIndex: number;
  rowsInCurrentSheet: number;
  totalRowsWritten: number;
  rowsPerSheet: number[];
  columns: ColumnMetadata[];
}

export function createExcelWriter(
  tableName: string,
  columns: ColumnMetadata[]
): ExcelWriterState {
  const workbook = new ExcelJS.Workbook();
  workbook.creator = 'Sage Data Extraction Tool';
  workbook.created = new Date();

  return {
    workbook,
    currentSheet: null,
    tableName,
    sheetIndex: 0,
    rowsInCurrentSheet: 0,
    totalRowsWritten: 0,
    rowsPerSheet: [],
    columns,
  };
}

function createNewSheet(state: ExcelWriterState): void {
  // Finalize previous sheet row count
  if (state.currentSheet && state.rowsInCurrentSheet > 0) {
    state.rowsPerSheet.push(state.rowsInCurrentSheet);
  }

  state.sheetIndex++;
  const sheetName =
    state.sheetIndex === 1
      ? state.tableName.slice(0, 31) // Excel sheet name limit
      : `${state.tableName.slice(0, 25)}_Part${state.sheetIndex}`;

  state.currentSheet = state.workbook.addWorksheet(sheetName);
  state.rowsInCurrentSheet = 0;

  // Add header row
  state.currentSheet.addRow(state.columns.map((c) => c.name));

  // Style header row
  const headerRow = state.currentSheet.getRow(1);
  headerRow.font = { bold: true };
  headerRow.fill = {
    type: 'pattern',
    pattern: 'solid',
    fgColor: { argb: 'FFE0E0E0' },
  };
}

export function writeRow(
  state: ExcelWriterState,
  row: Record<string, unknown>
): void {
  // Create first sheet or new sheet if at limit
  if (!state.currentSheet || state.rowsInCurrentSheet >= config.xlsx.maxRowsPerSheet) {
    createNewSheet(state);
  }

  const values = state.columns.map((col) => {
    const value = row[col.name];
    // Handle special types for Excel compatibility
    if (value === null || value === undefined) return null;
    if (value instanceof Date) return value;
    if (typeof value === 'object') return JSON.stringify(value);
    return value;
  });

  state.currentSheet!.addRow(values);
  state.rowsInCurrentSheet++;
  state.totalRowsWritten++;
}

export async function finalizeExcel(
  state: ExcelWriterState
): Promise<{ filePath: string; stats: ExtractionStats }> {
  // Record final sheet's row count
  if (state.rowsInCurrentSheet > 0) {
    state.rowsPerSheet.push(state.rowsInCurrentSheet);
  }

  // Ensure export directory exists
  const tableDir = path.join(config.paths.exports, state.tableName);
  if (!existsSync(tableDir)) {
    await mkdir(tableDir, { recursive: true });
  }

  const filePath = path.join(tableDir, 'data.xlsx');
  await state.workbook.xlsx.writeFile(filePath);

  const stats: ExtractionStats = {
    tableName: state.tableName,
    startTime: '', // Filled in by caller
    endTime: '',
    durationMs: 0,
    rowsWritten: state.totalRowsWritten,
    sheetCount: state.sheetIndex,
    rowsPerSheet: state.rowsPerSheet,
    warnings: [],
  };

  return { filePath, stats };
}

export async function writeSchemaFile(
  tableName: string,
  columns: ColumnMetadata[]
): Promise<void> {
  const tableDir = path.join(config.paths.exports, tableName);
  if (!existsSync(tableDir)) {
    await mkdir(tableDir, { recursive: true });
  }

  const schema: SchemaInfo = {
    tableName,
    columns,
    columnCount: columns.length,
    extractedAt: new Date().toISOString(),
  };

  await writeFile(
    path.join(tableDir, 'schema.json'),
    JSON.stringify(schema, null, 2)
  );
}

export async function writeStatsFile(
  tableName: string,
  stats: ExtractionStats
): Promise<void> {
  const tableDir = path.join(config.paths.exports, tableName);
  await writeFile(
    path.join(tableDir, 'stats.json'),
    JSON.stringify(stats, null, 2)
  );
}
