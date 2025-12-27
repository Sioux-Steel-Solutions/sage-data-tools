import ExcelJS from 'exceljs';
import { mkdir } from 'fs/promises';
import { existsSync, createWriteStream } from 'fs';
import path from 'path';
import { config } from './config.js';
import type { ColumnMetadata, ExtractionStats, SchemaInfo } from './types.js';
import { writeFile } from 'fs/promises';

export interface ExcelWriterState {
  workbook: ExcelJS.stream.xlsx.WorkbookWriter;
  currentSheet: ExcelJS.Worksheet | null;
  tableName: string;
  sheetIndex: number;
  rowsInCurrentSheet: number;
  totalRowsWritten: number;
  rowsPerSheet: number[];
  columns: ColumnMetadata[];
  filePath: string;
}

export async function createExcelWriter(
  tableName: string,
  columns: ColumnMetadata[]
): Promise<ExcelWriterState> {
  // Ensure export directory exists
  const tableDir = path.join(config.paths.exports, tableName);
  if (!existsSync(tableDir)) {
    await mkdir(tableDir, { recursive: true });
  }

  const filePath = path.join(tableDir, 'data.xlsx');

  // Use streaming writer to avoid memory issues with large tables
  const workbook = new ExcelJS.stream.xlsx.WorkbookWriter({
    filename: filePath,
    useStyles: true,
    useSharedStrings: false, // Disabled for better memory performance
  });

  (workbook as any).creator = 'Sage Data Extraction Tool';
  (workbook as any).created = new Date();

  return {
    workbook,
    currentSheet: null,
    tableName,
    sheetIndex: 0,
    rowsInCurrentSheet: 0,
    totalRowsWritten: 0,
    rowsPerSheet: [],
    columns,
    filePath,
  };
}

async function createNewSheet(state: ExcelWriterState): Promise<void> {
  // Finalize previous sheet row count and commit it
  if (state.currentSheet && state.rowsInCurrentSheet > 0) {
    state.rowsPerSheet.push(state.rowsInCurrentSheet);
    await state.currentSheet.commit();
  }

  state.sheetIndex++;
  const sheetName =
    state.sheetIndex === 1
      ? state.tableName.slice(0, 31) // Excel sheet name limit
      : `${state.tableName.slice(0, 25)}_Part${state.sheetIndex}`;

  state.currentSheet = state.workbook.addWorksheet(sheetName);
  state.rowsInCurrentSheet = 0;

  // Add header row
  const headerRow = state.currentSheet.addRow(state.columns.map((c) => c.name));
  headerRow.font = { bold: true };
  headerRow.fill = {
    type: 'pattern',
    pattern: 'solid',
    fgColor: { argb: 'FFE0E0E0' },
  };
  headerRow.commit();
}

export async function writeRow(
  state: ExcelWriterState,
  row: Record<string, unknown>
): Promise<void> {
  // Create first sheet or new sheet if at limit
  if (!state.currentSheet || state.rowsInCurrentSheet >= config.xlsx.maxRowsPerSheet) {
    await createNewSheet(state);
  }

  const values = state.columns.map((col) => {
    const value = row[col.name];
    // Handle special types for Excel compatibility
    if (value === null || value === undefined) return null;
    if (value instanceof Date) return value;
    if (typeof value === 'object') return JSON.stringify(value);
    return value;
  });

  const dataRow = state.currentSheet!.addRow(values);
  dataRow.commit();
  state.rowsInCurrentSheet++;
  state.totalRowsWritten++;
}

export async function finalizeExcel(
  state: ExcelWriterState
): Promise<{ filePath: string; stats: ExtractionStats }> {
  // Commit final sheet and record row count
  if (state.currentSheet) {
    if (state.rowsInCurrentSheet > 0) {
      state.rowsPerSheet.push(state.rowsInCurrentSheet);
    }
    await state.currentSheet.commit();
  }

  // Commit the workbook (finalizes the streaming write)
  await state.workbook.commit();

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

  return { filePath: state.filePath, stats };
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
