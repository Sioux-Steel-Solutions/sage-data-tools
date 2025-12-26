export type TableType = 'TABLE' | 'VIEW';

export type ProcessingStatus =
  | 'pending'
  | 'discovering'
  | 'discovered'
  | 'extracting'
  | 'extracted'
  | 'validating'
  | 'validated'
  | 'failed'
  | 'skipped';

export type ValidationResult =
  | 'VERIFIED'
  | 'ROW_COUNT_MISMATCH'
  | 'COLUMN_MISMATCH'
  | 'VALIDATION_FAILED'
  | 'NOT_VALIDATED';

export type UserDecision = 'continue' | 'retry' | 'abort';

export interface ColumnMetadata {
  name: string;
  index: number;
  type?: string;
  nullable?: boolean;
}

export interface TableEntry {
  name: string;
  type: TableType;
  status: ProcessingStatus;

  // Phase 0 - Discovery
  discoveredAt?: string;
  columns?: ColumnMetadata[];
  columnCount?: number;
  discoveryError?: string;

  // Phase 1 - Extraction
  extractionStartedAt?: string;
  extractionCompletedAt?: string;
  rowsExtracted?: number;
  sheetsCreated?: number;
  rowsPerSheet?: number[];
  extractionError?: string;

  // Phase 2 - Validation
  validationStartedAt?: string;
  validationCompletedAt?: string;
  validationResult?: ValidationResult;
  sourceRowCount?: number;
  validationError?: string;

  // Failure handling
  userDecision?: UserDecision;
  failurePhase?: 'discovery' | 'extraction' | 'validation';
  retryCount?: number;
}

export interface Manifest {
  version: string;
  createdAt: string;
  updatedAt: string;
  sourceDatabase: string;
  tables: TableEntry[];
  summary: {
    total: number;
    pending: number;
    discovered: number;
    extracted: number;
    validated: number;
    failed: number;
    skipped: number;
  };
}

export interface ExtractionStats {
  tableName: string;
  startTime: string;
  endTime: string;
  durationMs: number;
  rowsWritten: number;
  sheetCount: number;
  rowsPerSheet: number[];
  warnings: string[];
}

export interface SchemaInfo {
  tableName: string;
  columns: ColumnMetadata[];
  columnCount: number;
  extractedAt: string;
}
