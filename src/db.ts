import sql from 'mssql';
import { config } from './config.js';
import type { ColumnMetadata } from './types.js';

let pool: sql.ConnectionPool | null = null;

export async function connect(): Promise<sql.ConnectionPool> {
  if (pool) return pool;

  pool = await sql.connect({
    server: config.sql.host,
    port: config.sql.port,
    database: config.sql.database,
    user: config.sql.user,
    password: config.sql.password,
    options: config.sql.options,
    requestTimeout: 300000, // 5 minutes for long queries
    connectionTimeout: 30000,
  });

  return pool;
}

export async function disconnect(): Promise<void> {
  if (pool) {
    await pool.close();
    pool = null;
  }
}

export async function enumerateTables(): Promise<
  Array<{ name: string; type: 'TABLE' | 'VIEW' }>
> {
  const conn = await connect();
  const result = await conn.request().query(`EXEC sp_tables_ex 'SAGE'`);

  return result.recordset.map((row: Record<string, unknown>) => ({
    name: row.TABLE_NAME as string,
    type: (row.TABLE_TYPE as string) === 'VIEW' ? 'VIEW' : 'TABLE',
  }));
}

export async function smokeTestTable(
  tableName: string
): Promise<{ success: boolean; columns?: ColumnMetadata[]; error?: string }> {
  try {
    const conn = await connect();
    const query = `SELECT TOP 1 * FROM OPENQUERY(SAGE, 'SELECT * FROM ${tableName}')`;
    const result = await conn.request().query(query);

    const columns: ColumnMetadata[] = Object.keys(
      result.recordset[0] || {}
    ).map((name, index) => ({
      name,
      index,
    }));

    // If no rows, try to get columns from metadata
    if (result.recordset.length === 0 && result.recordset.columns) {
      const cols = result.recordset.columns as Record<string, { index: number; name: string; type: unknown }>;
      return {
        success: true,
        columns: Object.values(cols).map((col) => ({
          name: col.name,
          index: col.index,
          type: String(col.type),
        })),
      };
    }

    return { success: true, columns };
  } catch (err) {
    return {
      success: false,
      error: err instanceof Error ? err.message : String(err),
    };
  }
}

export async function* streamTableRows(
  tableName: string
): AsyncGenerator<Record<string, unknown>, void, unknown> {
  const conn = await connect();
  const request = conn.request();
  request.stream = true;

  const query = `SELECT * FROM OPENQUERY(SAGE, 'SELECT * FROM ${tableName}')`;

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
      yield await new Promise<Record<string, unknown>>((resolve) => {
        if (rows.length > 0) {
          resolve(rows.shift()!);
        } else if (done) {
          resolve(undefined as unknown as Record<string, unknown>);
        } else {
          resolveNext = (result) => {
            if (result.done) {
              resolve(undefined as unknown as Record<string, unknown>);
            } else {
              resolve(result.value);
            }
          };
        }
      });
    }
  }
}

export async function getTableRowCount(tableName: string): Promise<number> {
  const conn = await connect();
  const query = `SELECT COUNT(*) as cnt FROM OPENQUERY(SAGE, 'SELECT * FROM ${tableName}')`;
  const result = await conn.request().query(query);
  return result.recordset[0].cnt;
}
