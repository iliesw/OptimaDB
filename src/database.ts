import { Database } from "bun:sqlite";
import { TABLE_META } from "./schema";
import type { OptimaTableDef, OptimaTableMeta, OptimaField } from "./schema";
import * as fs from "node:fs";
import * as path from "node:path";

// ---------- Typing helpers for great DX ----------
export type FieldRuntimeType<F> = F extends OptimaField<infer T, any, any>
  ? T
  : any;
export type RowOf<TDef extends OptimaTableDef<Record<string, any>>> = {
  [K in keyof TDef]: FieldRuntimeType<TDef[K]>;
};
export type PartialRowOf<TDef extends OptimaTableDef<Record<string, any>>> =
  Partial<RowOf<TDef>>;

// Keys that are NOT NULL and do not have a default should be required in Insert
export type RequiredInsertKeys<
  TDef extends OptimaTableDef<Record<string, any>>
> = {
  [K in keyof TDef]-?: TDef[K] extends OptimaField<
    any,
    infer TNotNull extends boolean,
    any
  >
    ? TNotNull extends true
      ? K
      : never
    : never;
}[keyof TDef];

export type InsertInput<TDef extends OptimaTableDef<Record<string, any>>> =
  Pick<RowOf<TDef>, RequiredInsertKeys<TDef>> &
    Partial<Omit<RowOf<TDef>, RequiredInsertKeys<TDef>>>;

export type WhereOperatorObject<T> = {
  $eq?: T;
  $ne?: T | null;
  $gt?: T;
  $gte?: T;
  $lt?: T;
  $lte?: T;
  $like?: string;
  $between?: [T, T];
  $in?: T[];
  $nin?: T[];
  $is?: null | "null" | "not-null";
  $not?: WhereOperatorObject<T> | T | T[] | null;
};
export type ColumnWhere<T> = T | null | T[] | WhereOperatorObject<T>;
export type BasicWhere<TDef extends OptimaTableDef<Record<string, any>>> = {
  [K in keyof TDef]?: ColumnWhere<FieldRuntimeType<TDef[K]>>;
};
export type RawWhere = string | { $raw: { sql: string; params?: any[] } };
export type WhereInput<TDef extends OptimaTableDef<Record<string, any>>> =
  | RawWhere
  | (BasicWhere<TDef> & {
      $or?: WhereInput<TDef>[];
      $and?: WhereInput<TDef>[];
    });

export type GetOptions<TDef extends OptimaTableDef<Record<string, any>>> = {
  Limit?: number;
  Offset?: number;
  Unique?: boolean;
  Extend?: string[] | string;
  OrderBy?: {
    Column: keyof RowOf<TDef> & string;
    Direction: "ASC" | "DESC";
  };
};

type OptimaTablesFromSchema<S extends Record<string, OptimaTableDef<any>>> = {
  [K in keyof S]: OptimaTable<S[K]>;
};

export class OptimaDB<S extends Record<string, OptimaTableDef<any>>> {
  private Path: string = "";
  private InternalDB: Database;
  public Tables: OptimaTablesFromSchema<S>;

  // Hybrid/Mode support
  private Mode: "disk" | "memory" | "hybrid" = "memory";
  private SchemaRef: S;
  private SaveDebounceTimer: any = null;
  private SaveIntervalTimer: any = null;
  private AutosaveEnabled: boolean = false;
  private AutosaveDebounceMs: number = 1500;
  private AutosaveIntervalMs: number = 30000;
  private onSigInt?: () => void;
  private onSigTerm?: () => void;
  private onBeforeExit?: () => void;
  private onUncaughtException?: (err: any) => void;

  constructor(
    Schema: S,
    options?:
      | string
      | {
          mode?: "disk" | "memory" | "hybrid";
          path?: string;
          autosave?: {
            enabled?: boolean;
            debounceMs?: number;
            intervalMs?: number;
          };
        }
  ) {
    this.SchemaRef = Schema;

    // Interpret constructor overload
    if (typeof options === "string") {
      this.Mode = "disk";
      this.Path = options;
    } else if (typeof options === "object" && options) {
      this.Mode = options.mode ?? (options.path ? "disk" : "memory");
      this.Path = options.path ?? "";
      if (options.autosave) {
        this.AutosaveEnabled = options.autosave.enabled ?? true;
        if (typeof options.autosave.debounceMs === "number")
          this.AutosaveDebounceMs = options.autosave.debounceMs;
        if (typeof options.autosave.intervalMs === "number")
          this.AutosaveIntervalMs = options.autosave.intervalMs;
      } else {
        this.AutosaveEnabled = this.Mode === "hybrid";
      }
    } else {
      this.Mode = "memory";
      this.Path = "";
      this.AutosaveEnabled = false;
    }

    // Open the primary database connection
    if (this.Mode === "disk") {
      const dbFile = this.ensureDbPath();
      this.InternalDB = new Database(dbFile, { create: true });
    } else {
      this.InternalDB = new Database(":memory:");
    }
    this.InternalDB.exec("PRAGMA journal_mode = WAL;"); // Enable WAL MODE
    // Help avoid SQLITE_BUSY on Windows when other processes touch the file
    try { this.InternalDB.exec("PRAGMA busy_timeout = 5000;"); } catch {}

    const tables = {} as OptimaTablesFromSchema<S>;
    for (const tableName in Schema) {
      if (
        Object.prototype.hasOwnProperty.call(Schema, tableName) &&
        Schema[tableName]
      ) {
        const tableDef = Schema[tableName as keyof S] as S[keyof S];
        (tables as any)[tableName] = new OptimaTable(
          this.InternalDB,
          tableName,
          tableDef,
          Schema,
          this
        );
      }
    }
    this.Tables = tables;

    // Auto-migrate at startup to match in-code schema (no renames by default)
    for (const tableName in this.Tables) {
      const t = (this.Tables as any)[tableName] as OptimaTable<any>;
      t.MigrateSchema();
    }

    // If hybrid, import from disk and start autosave timers/handlers
    if (this.Mode === "hybrid") {
      this.loadFromDiskIfExists();
      this.startAutosaveInterval();
      this.installSignalHandlers();
    }
  }

  Close = () => {
    // Stop background activity first to avoid races
    try {
      if (this.SaveDebounceTimer) clearTimeout(this.SaveDebounceTimer);
      if (this.SaveIntervalTimer) clearInterval(this.SaveIntervalTimer);
      this.SaveDebounceTimer = null;
      this.SaveIntervalTimer = null;
    } catch {}
    this.uninstallSignalHandlers();
    try {
      if (this.Mode === "hybrid") {
        this.saveToDiskSafe();
        // Open the saved file briefly to fully release WAL/SHM on Windows
        try {
          const finalFile = path.join(this.Path ? this.Path : ".", "db.sqlite");
          if (fs.existsSync(finalFile)) {
            const db = new Database(finalFile, { create: false });
            try { db.exec("PRAGMA busy_timeout = 5000;"); } catch {}
            try { db.exec("PRAGMA wal_checkpoint(TRUNCATE);"); } catch {}
            try { db.exec("PRAGMA journal_mode = DELETE;"); } catch {}
            try { db.exec("PRAGMA optimize;"); } catch {}
            try { db.close(true); } catch {}
          }
        } catch {}
      }
    } catch {
      // ignore close-time save errors
    }
    // On Windows, WAL sidecar files can appear "locked" if not checkpointed.
    // Checkpoint and switch back to DELETE journaling before closing the handle.
    try {
      if (this.Mode === "disk") {
        try { this.InternalDB.exec("PRAGMA wal_checkpoint(TRUNCATE);"); } catch {}
        try { this.InternalDB.exec("PRAGMA journal_mode = DELETE;"); } catch {}
        try { this.InternalDB.exec("PRAGMA optimize;"); } catch {}
      }
    } catch {}
    // Retry-close if busy
    let lastErr: any;
    for (let i = 0; i < 3; i++) {
      try { this.InternalDB.close(true); return; }
      catch (e) {
        lastErr = e;
        Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 50);
      }
    }
    // throw last error if still failing
    throw lastErr;
  };

  /**
   * Migrate all tables to match the provided in-code schema.
   * Pass optional rename maps: { [tableName]: { oldCol: newCol } }
   */
  Migrate = (renames?: Partial<{ [K in keyof S & string]: Record<string, string> }>) => {
    for (const tableName in this.Tables) {
      const table = (this.Tables as any)[tableName] as OptimaTable<any>;
      const renameMap = (renames as any)?.[tableName] as Record<string, string> | undefined;
      table.MigrateSchema(renameMap);
    }
  };

  // Manual snapshot to disk (hybrid only). No-op in other modes.
  public SaveNow(): void {
    this.saveToDiskSafe();
  }

  // ---------- Hybrid helpers ----------
  private ensureDbPath(): string {
    const base = this.Path ? this.Path : ".";
    try { fs.mkdirSync(base, { recursive: true }); } catch {}
    return path.join(base, "db.sqlite");
  }

  private escapeSqlStringLiteral(p: string): string {
    return p.replace(/'/g, "''");
  }

  private defaultLiteralForField(field: any): string {
    try {
      const hasDefault = typeof field?.hasDefaultValue === "function" ? field.hasDefaultValue() : false;
      if (!hasDefault) return "NULL";
      const rawDefault = typeof field?.getDefaultValue === "function" ? field.getDefaultValue() : undefined;
      const formatted = typeof field?.applyFormatIn === "function" ? field.applyFormatIn(rawDefault) : rawDefault;
      if (formatted === null || formatted === undefined) return "NULL";
      if (typeof formatted === "number") return String(formatted);
      if (typeof formatted === "boolean") return formatted ? "1" : "0";
      const asString = String(formatted);
      const escaped = asString.replace(/'/g, "''");
      return `'${escaped}'`;
    } catch {
      return "NULL";
    }
  }

  private loadFromDiskIfExists() {
    if (!this.Path) return;
    const dbFile = this.ensureDbPath();
    if (!fs.existsSync(dbFile)) return;

    const escaped = this.escapeSqlStringLiteral(dbFile);
    this.InternalDB.run("BEGIN");
    try {
      this.InternalDB.exec(`ATTACH '${escaped}' AS disk;`);
      // For each table defined in the schema, import from disk if present
      for (const tableName in this.Tables) {
        const desiredSchema = (this.SchemaRef as any)[tableName];
        if (!desiredSchema) continue;

        const existsRow = this.InternalDB
          .query("SELECT name FROM disk.sqlite_master WHERE type='table' AND name = ?")
          .get(tableName) as { name?: string } | undefined;
        if (!existsRow || existsRow.name !== tableName) continue;

        const rows = this.InternalDB
          .query(`PRAGMA disk.table_info("${tableName}")`)
          .all() as Array<{ name: string }>;
        const diskCols = rows.map(r => r.name);
        const desiredCols = Object.keys(desiredSchema);

        const selectExprs: string[] = [];
        const targetCols: string[] = [];
        for (const col of desiredCols) {
          targetCols.push(`"${col}"`);
          if (diskCols.includes(col)) {
            selectExprs.push(`"${col}"`);
          } else {
            const field = (desiredSchema as any)[col];
            const literal = this.defaultLiteralForField(field);
            selectExprs.push(`${literal} AS "${col}"`);
          }
        }

        // Import data
        const insertSQL = `INSERT INTO "${tableName}" (${targetCols.join(", ")}) SELECT ${selectExprs.join(", ")} FROM disk."${tableName}"`;
        this.InternalDB.exec(insertSQL);
      }
      // Commit the import transaction before detaching the file DB
      this.InternalDB.run("COMMIT");
      // After commit, ensure attached DB is checkpointed and not left in WAL
      try { this.InternalDB.exec("PRAGMA disk.wal_checkpoint(TRUNCATE);"); } catch {}
      try { this.InternalDB.exec("PRAGMA disk.journal_mode = DELETE;"); } catch {}
      this.InternalDB.exec("DETACH disk;");
    } catch (e) {
      try { this.InternalDB.run("ROLLBACK"); } catch {}
      try { this.InternalDB.exec("DETACH disk;"); } catch {}
      throw e;
    }
  }

  private saveToDiskViaVacuum(): void {
    const base = this.Path ? this.Path : ".";
    try { fs.mkdirSync(base, { recursive: true }); } catch {}
    const tmpFile = path.join(base, "db.sqlite.tmp");
    const finalFile = path.join(base, "db.sqlite");
    const escapedTmp = this.escapeSqlStringLiteral(tmpFile);
    this.InternalDB.exec(`VACUUM INTO '${escapedTmp}';`);
    try {
      if (fs.existsSync(finalFile)) fs.rmSync(finalFile, { force: true });
    } catch {}
    fs.renameSync(tmpFile, finalFile);
    // Post-process the written file to avoid leftover WAL/SHM on Windows
    try {
      const tmpDb = new Database(finalFile, { create: true });
      try { tmpDb.exec("PRAGMA busy_timeout = 5000;"); } catch {}
      try { tmpDb.exec("PRAGMA wal_checkpoint(TRUNCATE);"); } catch {}
      try { tmpDb.exec("PRAGMA journal_mode = DELETE;"); } catch {}
      try { tmpDb.exec("PRAGMA optimize;"); } catch {}
      try { tmpDb.close(true); } catch {}
    } catch {}
  }

  private saveToDiskViaAttach(): void {
    const base = this.Path ? this.Path : ".";
    try { fs.mkdirSync(base, { recursive: true }); } catch {}
    const finalFile = path.join(base, "db.sqlite");
    const escaped = this.escapeSqlStringLiteral(finalFile);

    this.InternalDB.run("BEGIN");
    try {
      this.InternalDB.exec(`ATTACH '${escaped}' AS disk;`);
      // Recreate each table on disk with current schema and copy data
      for (const tableName in this.Tables) {
        const desiredSchema = (this.SchemaRef as any)[tableName];
        if (!desiredSchema) continue;

        // Drop existing disk table and recreate
        this.InternalDB.exec(`DROP TABLE IF EXISTS disk."${tableName}";`);
        const colDefs = Object.entries(desiredSchema).map(([colName, field]: any) => `"${colName}" ${field["toSQL"]?.()}`);
        const createSQL = `CREATE TABLE disk."${tableName}" (\n  ${colDefs.join(",\n  ")}\n);`;
        this.InternalDB.exec(createSQL);

        const cols = Object.keys(desiredSchema).map(c => `"${c}"`).join(", ");
        this.InternalDB.exec(`INSERT INTO disk."${tableName}" (${cols}) SELECT ${cols} FROM main."${tableName}";`);
      }
      // Commit first, then run pragmas and detach outside the transaction
      this.InternalDB.run("COMMIT");
      try { this.InternalDB.exec("PRAGMA disk.wal_checkpoint(TRUNCATE);"); } catch {}
      try { this.InternalDB.exec("PRAGMA disk.journal_mode = DELETE;"); } catch {}
      this.InternalDB.exec("DETACH disk;");
    } catch (e) {
      try { this.InternalDB.run("ROLLBACK"); } catch {}
      try { this.InternalDB.exec("DETACH disk;"); } catch {}
      throw e;
    }
  }

  private saveToDisk(): void {
    if (this.Mode !== "hybrid") return;
    try {
      this.saveToDiskViaVacuum();
    } catch {
      this.saveToDiskViaAttach();
    }
  }

  private saveToDiskSafe(): void {
    try { this.saveToDisk(); } catch (e) { /* swallow */ }
  }

  public scheduleSave(): void {
    if (this.Mode !== "hybrid") return;
    if (!this.AutosaveEnabled) return;
    try { if (this.SaveDebounceTimer) clearTimeout(this.SaveDebounceTimer); } catch {}
    this.SaveDebounceTimer = setTimeout(() => {
      this.saveToDiskSafe();
    }, this.AutosaveDebounceMs);
  }

  private startAutosaveInterval(): void {
    if (this.Mode !== "hybrid") return;
    if (!this.AutosaveEnabled) return;
    if (this.AutosaveIntervalMs && this.AutosaveIntervalMs > 0) {
      try { if (this.SaveIntervalTimer) clearInterval(this.SaveIntervalTimer); } catch {}
      this.SaveIntervalTimer = setInterval(() => this.saveToDiskSafe(), this.AutosaveIntervalMs);
    }
  }

  private installSignalHandlers(): void {
    const attempt = () => { try { this.saveToDiskSafe(); } catch {} };
    this.onSigInt = attempt;
    this.onSigTerm = attempt;
    this.onBeforeExit = attempt;
    this.onUncaughtException = (err: any) => { try { this.saveToDiskSafe(); } catch {}; };
    try {
      (process as any)?.on?.("SIGINT", this.onSigInt);
      (process as any)?.on?.("SIGTERM", this.onSigTerm);
      (process as any)?.on?.("beforeExit", this.onBeforeExit);
      (process as any)?.on?.("uncaughtException", this.onUncaughtException);
    } catch {}
  }

  private uninstallSignalHandlers(): void {
    try {
      if (this.onSigInt) (process as any)?.off?.("SIGINT", this.onSigInt);
      if (this.onSigTerm) (process as any)?.off?.("SIGTERM", this.onSigTerm);
      if (this.onBeforeExit) (process as any)?.off?.("beforeExit", this.onBeforeExit);
      if (this.onUncaughtException) (process as any)?.off?.("uncaughtException", this.onUncaughtException);
    } catch {}
    this.onSigInt = undefined;
    this.onSigTerm = undefined;
    this.onBeforeExit = undefined;
    this.onUncaughtException = undefined;
  }
}

export class OptimaTable<TDef extends OptimaTableDef<Record<string, any>>> {
  private Name: string = "UNKNOWN";
  private InternalDBRefrance: Database;
  private InternalOptimaDBRefrance: OptimaDB<any>;

  private Schema: TDef;
  private Meta: OptimaTableMeta;
  private extendRelationships: Map<string, any> = new Map();
  private InitTable(Tables: any) {
    this.InternalDBRefrance.query(this.Meta.toSQL()).run();

    // PreComute Relations
    for (const table of Object.keys(Tables)) {
      if (table == this.Name) continue;
      const tableSchema = (Tables as Record<string, any>)[table];
      for (const Column of Object.keys(tableSchema)) {
        const ColumnSchema = tableSchema[Column];
        if (
          ColumnSchema.Reference &&
          ColumnSchema.Reference.Table == this.Name
        ) {
          this.extendRelationships.set(table, {
            ExternalField: Column,
            InternalField: ColumnSchema.Reference.Field,
            Type: ColumnSchema.Reference.Type,
          });
        }
      }
    }
  }
  constructor(
    InternalDB: Database,
    TableName: string,
    SchemaTable: TDef,
    TablesToCompute: any,
    OptimaDBRef: OptimaDB<any>
  ) {
    this.InternalDBRefrance = InternalDB;
    this.InternalOptimaDBRefrance = OptimaDBRef;
    this.Schema = SchemaTable;
    this.Meta = (SchemaTable as any)[TABLE_META] as OptimaTableMeta;
    this.Name = this.Meta?.Name || TableName;
    this.InitTable(TablesToCompute);
  }

  private mapOutRow = (row: any) => {
    if (!row) return row;
    const result: Record<string, any> = {};
    for (const key of Object.keys(row)) {
      const field = (this.Schema as any)[key];
      if (field && typeof field.applyFormatOut === "function") {
        result[key] = field.applyFormatOut(row[key]);
      } else {
        result[key] = row[key];
      }
    }
    return result;
  };

  private buildWhereClause = (where?: any) => {
    // No where or empty object → no clause
    if (
      !where ||
      (typeof where === "object" && Object.keys(where).length === 0)
    ) {
      return { clause: "", params: [] as any[] } as const;
    }

    // Raw string where
    if (typeof where === "string") {
      return { clause: ` WHERE ${where}`, params: [] as any[] } as const;
    }

    // Raw with params: { $raw: { sql: string, params?: any[] } }
    if (where && typeof where === "object" && "$raw" in where) {
      const raw = where.$raw || {};
      const sql: string = raw.sql || "";
      const params: any[] = Array.isArray(raw.params) ? raw.params : [];
      if (!sql) return { clause: "", params: [] } as const;
      return { clause: ` WHERE ${sql}`, params } as const;
    }

    const buildForObject = (
      obj: Record<string, any>
    ): { part: string; params: any[] } => {
      const parts: string[] = [];
      const params: any[] = [];

      const ensureFormatted = (column: string, value: any) => {
        const field = (this.Schema as any)[column];
        if (field && typeof field.applyFormatIn === "function") {
          return field.applyFormatIn(value);
        }
        return value;
      };

      const compileColumnOp = (column: string, op: string, value: any) => {
        switch (op) {
          case "$eq":
            parts.push(`"${column}" = ?`);
            params.push(ensureFormatted(column, value));
            break;
          case "$ne":
            if (value === null) {
              parts.push(`"${column}" IS NOT NULL`);
            } else {
              parts.push(`"${column}" <> ?`);
              params.push(ensureFormatted(column, value));
            }
            break;
          case "$gt":
            parts.push(`"${column}" > ?`);
            params.push(ensureFormatted(column, value));
            break;
          case "$gte":
            parts.push(`"${column}" >= ?`);
            params.push(ensureFormatted(column, value));
            break;
          case "$lt":
            parts.push(`"${column}" < ?`);
            params.push(ensureFormatted(column, value));
            break;
          case "$lte":
            parts.push(`"${column}" <= ?`);
            params.push(ensureFormatted(column, value));
            break;
          case "$like":
            parts.push(`"${column}" LIKE ?`);
            params.push(ensureFormatted(column, value));
            break;
          case "$between": {
            const [a, b] = Array.isArray(value) ? value : [];
            parts.push(`"${column}" BETWEEN ? AND ?`);
            params.push(ensureFormatted(column, a), ensureFormatted(column, b));
            break;
          }
          case "$in": {
            const arr: any[] = Array.isArray(value) ? value : [];
            if (arr.length === 0) {
              parts.push("1 = 0");
            } else {
              const placeholders = arr.map(() => "?").join(", ");
              parts.push(`"${column}" IN (${placeholders})`);
              params.push(...arr.map((v) => ensureFormatted(column, v)));
            }
            break;
          }
          case "$nin": {
            const arr: any[] = Array.isArray(value) ? value : [];
            if (arr.length === 0) {
              parts.push("1 = 1");
            } else {
              const placeholders = arr.map(() => "?").join(", ");
              parts.push(`"${column}" NOT IN (${placeholders})`);
              params.push(...arr.map((v) => ensureFormatted(column, v)));
            }
            break;
          }
          case "$is": {
            if (value === null || value === "null") {
              parts.push(`"${column}" IS NULL`);
            } else {
              parts.push(`"${column}" IS NOT NULL`);
            }
            break;
          }
          case "$not": {
            // $not against a simple value or an operator object
            if (
              value !== null &&
              typeof value === "object" &&
              !Array.isArray(value)
            ) {
              // Build a single combined expression for this column and negate it
              const compiled = buildForObject({ [column]: value });
              if (compiled.part) {
                parts.push(`NOT (${compiled.part})`);
                params.push(...compiled.params);
              }
            } else if (Array.isArray(value)) {
              const placeholders = value.map(() => "?").join(", ");
              parts.push(`NOT ("${column}" IN (${placeholders}))`);
              params.push(...value.map((v) => ensureFormatted(column, v)));
            } else if (value === null) {
              parts.push(`"${column}" IS NOT NULL`);
            } else {
              parts.push(`NOT ("${column}" = ?)`);
              params.push(ensureFormatted(column, value));
            }
            break;
          }
          default: {
            // Fallback to equality if unknown operator
            parts.push(`"${column}" = ?`);
            params.push(ensureFormatted(column, value));
            break;
          }
        }
      };

      const processNode = (key: string, value: any) => {
        if (key === "$or" || key === "$and") {
          const joiner = key === "$or" ? "OR" : "AND";
          const arr = Array.isArray(value) ? value : [value];
          const subParts: string[] = [];
          const subParams: any[] = [];
          for (const sub of arr) {
            const compiled = buildForObject(sub);
            if (compiled.part) {
              subParts.push(`(${compiled.part})`);
              subParams.push(...compiled.params);
            }
          }
          if (subParts.length > 0) {
            parts.push(subParts.join(` ${joiner} `));
            params.push(...subParams);
          }
          return;
        }

        // Column case
        const column = key;
        const valueRef = value;
        if (valueRef === null) {
          parts.push(`"${column}" IS NULL`);
          return;
        }
        if (Array.isArray(valueRef)) {
          if (valueRef.length === 0) {
            parts.push("1 = 0");
          } else {
            const placeholders = valueRef.map(() => "?").join(", ");
            parts.push(`"${column}" IN (${placeholders})`);
            params.push(...valueRef.map((v) => ensureFormatted(column, v)));
          }
          return;
        }
        if (typeof valueRef === "object") {
          for (const [op, v] of Object.entries(valueRef)) {
            compileColumnOp(column, op, v);
          }
          return;
        }
        // Scalar equality
        parts.push(`"${column}" = ?`);
        params.push(ensureFormatted(column, valueRef));
      };

      for (const [key, value] of Object.entries(obj)) {
        processNode(key, value);
      }

      return { part: parts.join(" AND "), params };
    };

    const compiled = buildForObject(where as Record<string, any>);
    if (!compiled.part) return { clause: "", params: [] } as const;
    return {
      clause: ` WHERE ${compiled.part}`,
      params: compiled.params,
    } as const;
  };
  /**
   * Fetch rows optionally filtered by a typed WHERE object.
   * - Supports operators like $gt, $in, $or, etc.
   */
  Get = (
    where?: WhereInput<TDef>,
    options?: GetOptions<TDef>
  ): RowOf<TDef>[] => {
    const { clause, params } = this.buildWhereClause(where as any);
    const selectPrefix = options?.Unique ? "SELECT DISTINCT *" : "SELECT *";
    let orderClause = "";
    if (options?.OrderBy) {
      const dir = options.OrderBy.Direction === "DESC" ? "DESC" : "ASC";
      orderClause = ` ORDER BY "${options.OrderBy.Column}" ${dir}`;
    }
    let limitClause = "";
    const extraParams: any[] = [];
    if (options?.Limit !== undefined) {
      limitClause += " LIMIT ?";
      extraParams.push(Number(options.Limit));
      if (options?.Offset !== undefined) {
        limitClause += " OFFSET ?";
        extraParams.push(Number(options.Offset));
      }
    } else if (options?.Offset !== undefined) {
      // Support OFFSET without LIMIT by using LIMIT -1 (SQLite idiom)
      limitClause += " LIMIT -1 OFFSET ?";
      extraParams.push(Number(options.Offset));
    }

    const sql = `${selectPrefix} FROM "${this.Name}"${clause}${orderClause}${limitClause}`;
    const rows = this.InternalDBRefrance.query(sql).all(
      ...params,
      ...extraParams
    );
    let CleenRows = rows.map((r: any) => this.mapOutRow(r));
    if (options?.Extend != undefined) {
      if (Array.isArray(options.Extend)) {
        options.Extend.forEach((e) => {
          if (this.extendRelationships.has(e)) {
            CleenRows = CleenRows.map((r: any) => {
              const Table = e as string;
              const Relation = this.extendRelationships.get(Table);
              let Data;
              if (Relation.Type == "MANY") {
                Data = this.InternalOptimaDBRefrance.Tables[Table]?.Get({
                  [Relation.ExternalField]: r[Relation.InternalField],
                });
              } else {
                Data = this.InternalOptimaDBRefrance.Tables[Table]?.GetOne({
                  [Relation.ExternalField]: r[Relation.InternalField],
                });
              }
              return { ...r, ["$" + e]: Data };
            });
          } else {
            throw new Error(
              "Table " +
                this.Name +
                " Doesn't have a realation to the table " +
                e
            );
          }
        });
      } else {
        if (this.extendRelationships.has(options.Extend)) {
          CleenRows = CleenRows.map((r: any) => {
            const Table = options.Extend as string;
            const Relation = this.extendRelationships.get(Table);
            let Data;
            if (Relation.Type == "MANY") {
              Data = this.InternalOptimaDBRefrance.Tables[Table]?.Get({
                [Relation.ExternalField]: r[Relation.InternalField],
              });
            } else {
              Data = this.InternalOptimaDBRefrance.Tables[Table]?.GetOne({
                [Relation.ExternalField]: r[Relation.InternalField],
              });
            }
            return { ...r, ["$" + options.Extend]: Data };
          });
        } else {
          throw new Error(
            "Table " +
              this.Name +
              " Doesn't have a realation to the table " +
              options.Extend
          );
        }
      }
    }
    return CleenRows;
  };
  /**
   * Fetch a single row matching the optional WHERE filter.
   */
  GetOne = (where?: WhereInput<TDef>): RowOf<TDef> | undefined => {
    const { clause, params } = this.buildWhereClause(where as any);
    const row = this.InternalDBRefrance.query(
      `SELECT * FROM "${this.Name}"${clause}`
    ).get(...params);
    return this.mapOutRow(row);
  };
  Insert = (Values: InsertInput<TDef>) => {
    // Runtime validation: ensure all NOT NULL fields are present and non-null
    for (const key of Object.keys(this.Schema)) {
      const field = (this.Schema as any)[key] as OptimaField<any> & {
        isNotNullField?: () => boolean;
      };
      const valueProvided = Object.prototype.hasOwnProperty.call(Values, key);
      const notNull = field?.isNotNullField?.() === true;
      if (notNull) {
        if (!valueProvided) {
          throw new Error(
            `Missing required field: ${String(key)} on insert into ${this.Name}`
          );
        }
        if (
          (Values as any)[key] === null ||
          (Values as any)[key] === undefined
        ) {
          throw new Error(
            `Field ${String(
              key
            )} is NOT NULL and must be provided with a non-null value in ${
              this.Name
            }`
          );
        }
      }
    }
    const columns = Object.keys(Values as Record<string, unknown>);
    const placeholders = columns.map(() => "?").join(", ");
    const sql = `INSERT INTO "${this.Name}" (${columns
      .map((col) => `"${col}"`)
      .join(", ")}) VALUES (${placeholders})`;
    const stmt = this.InternalDBRefrance.prepare(sql);
    const formattedValues = columns.map((col) => {
      const field = (this.Schema as any)[col];
      const raw = (Values as any)[col];
      if (field && typeof field.applyFormatIn === "function") {
        return field.applyFormatIn(raw);
      }
      return raw;
    });
    const res = stmt.run(...formattedValues);
    try { this.InternalOptimaDBRefrance.scheduleSave?.(); } catch {}
    return res;
  };

  /**
   * Update rows with typed values and WHERE conditions.
   */
  Update = (values: PartialRowOf<TDef>, where?: WhereInput<TDef>) => {
    // For updates, if a NOT NULL field is explicitly set to null, block it
    for (const key of Object.keys(values)) {
      const field = (this.Schema as any)[key] as OptimaField<any> & {
        isNotNullField?: () => boolean;
      };
      if (field?.isNotNullField?.() && (values as any)[key] === null) {
        throw new Error(
          `Field ${String(key)} is NOT NULL and cannot be set to null in ${
            this.Name
          }`
        );
      }
    }
    const columns = Object.keys(values);
    if (columns.length === 0) return { changes: 0 } as any;

    const setParts: string[] = [];
    const setParams: any[] = [];
    for (const col of columns) {
      const field = (this.Schema as any)[col];
      const raw = values[col];
      const formatted =
        field && typeof field.applyFormatIn === "function"
          ? field.applyFormatIn(raw)
          : raw;
      setParts.push(`"${col}" = ?`);
      setParams.push(formatted);
    }

    const whereBuilt = this.buildWhereClause(where as any);
    const sql = `UPDATE "${this.Name}" SET ${setParts.join(", ")}${
      whereBuilt.clause
    }`;
    const stmt = this.InternalDBRefrance.prepare(sql);
    const res = stmt.run(...setParams, ...whereBuilt.params);
    try { this.InternalOptimaDBRefrance.scheduleSave?.(); } catch {}
    return res;
  };

  /**
   * Delete rows matching the typed WHERE conditions.
   */
  Delete = (where?: WhereInput<TDef>) => {
    const whereBuilt = this.buildWhereClause(where as any);
    const sql = `DELETE FROM "${this.Name}"${whereBuilt.clause}`;
    const stmt = this.InternalDBRefrance.prepare(sql);
    const res = stmt.run(...whereBuilt.params);
    try { this.InternalOptimaDBRefrance.scheduleSave?.(); } catch {}
    return res;
  };
  /**
   * Count rows, optionally with typed WHERE.
   */
  Count = (where?: WhereInput<TDef>) => {
    const { clause, params } = this.buildWhereClause(where as any);
    const row = this.InternalDBRefrance.query(
      `SELECT COUNT(*) as count FROM "${this.Name}"${clause}`
    ).get(...params) as { count: number };
    return row ? row.count : 0;
  };
  Batch = (fn: Function) => {
    this.InternalDBRefrance.run("BEGIN");
    try { fn(); this.InternalDBRefrance.run("COMMIT"); try { this.InternalOptimaDBRefrance.scheduleSave?.(); } catch {} }
    catch (e) { this.InternalDBRefrance.run("ROLLBACK"); throw e; }
  };

  /**
   * Returns true if the underlying table exists in the database.
   */
  private tableExists = (): boolean => {
    const row = this.InternalDBRefrance
      .query("SELECT name FROM sqlite_master WHERE type='table' AND name = ?")
      .get(this.Name) as { name?: string } | undefined;
    return !!row && row.name === this.Name;
  };

  /**
   * Get current columns for this table from the database using PRAGMA table_info.
   */
  private getExistingColumns = () => {
    if (!this.tableExists()) return [] as Array<{ name: string; type: string | null; notnull: number; dflt_value: any; pk: number }>;
    const rows = this.InternalDBRefrance
      .query(`PRAGMA table_info("${this.Name}")`)
      .all() as Array<{ cid: number; name: string; type: string | null; notnull: number; dflt_value: any; pk: number }>;
    return rows;
  };

  /**
   * Build a CREATE TABLE statement for a specific name using the in-code schema definition.
   */
  private buildCreateSQLFor = (name: string): string => {
    const colDefs = Object.entries(this.Schema).map(([colName, field]) => {
      // Access the internal SQL builder the same way Table() does
      const def = (field as any)["toSQL"]?.();
      return `"${colName}" ${def}`;
    });
    return `CREATE TABLE "${name}" (\n  ${colDefs.join(",\n  ")}\n);`;
  };

  /**
   * Attempt to derive a SQL literal for a field's default value suitable for SELECT expressions.
   */
  private defaultLiteralForField = (field: any): string => {
    try {
      const hasDefault = typeof field?.hasDefaultValue === "function" ? field.hasDefaultValue() : false;
      if (!hasDefault) return "NULL";
      const rawDefault = typeof field?.getDefaultValue === "function" ? field.getDefaultValue() : undefined;
      const formatted = typeof field?.applyFormatIn === "function" ? field.applyFormatIn(rawDefault) : rawDefault;
      if (formatted === null || formatted === undefined) return "NULL";
      if (typeof formatted === "number") return String(formatted);
      if (typeof formatted === "boolean") return formatted ? "1" : "0";
      // Treat everything else as string
      const asString = String(formatted);
      const escaped = asString.replace(/'/g, "''");
      return `'${escaped}'`;
    } catch {
      return "NULL";
    }
  };

  /**
   * Migrate this table to match the in-code schema. Supports:
   * - Column renames via rename map (oldName -> newName)
   * - Column additions (new columns get their default or NULL)
   * - Column deletions (data for those columns is dropped)
   *
   * Strategy: Rebuild table with the desired schema, copy data, drop old, rename new.
   */
  MigrateSchema = (renameColumns?: Record<string, string>) => {
    const existing = this.getExistingColumns();
    const desiredColumns = Object.keys(this.Schema);

    // If table doesn't exist yet, just create it using the normal initializer and return.
    if (!this.tableExists()) {
      this.InternalDBRefrance.exec(this.buildCreateSQLFor(this.Name));
      return;
    }

    // Build rename maps in both directions for convenience
    const oldToNew = new Map<string, string>();
    const newToOld = new Map<string, string>();
    if (renameColumns) {
      for (const [oldName, newName] of Object.entries(renameColumns)) {
        oldToNew.set(oldName, newName);
        newToOld.set(newName, oldName);
      }
    }

    const existingNames = new Set(existing.map((c) => c.name));

    // Detect if a rebuild is necessary by comparing column sets (after considering renames)
    const normalizedExistingNames = new Set(
      Array.from(existingNames).map((n) => oldToNew.get(n) ?? n)
    );
    const desiredNameSet = new Set(desiredColumns);

    let requiresRebuild = false;
    // Check name-level differences
    for (const n of desiredNameSet) if (!normalizedExistingNames.has(n)) requiresRebuild = true;
    for (const n of normalizedExistingNames) if (!desiredNameSet.has(n)) requiresRebuild = true;

    // If sets match, we could still differ on constraints/types; rebuild to be safe.
    // To avoid unnecessary churn, you could diff types via PRAGMA table_info, but we prefer safety here.
    // If you want to skip in that case, comment the next line.
    // Keep rebuild on by default to ensure constraints are applied.
    requiresRebuild = requiresRebuild || true;

    if (!requiresRebuild) return;

    const tempName = `__tmp__${this.Name}`;

    this.InternalDBRefrance.run("BEGIN");
    try {
      this.InternalDBRefrance.run("PRAGMA foreign_keys = OFF");

      // Create temp table with desired schema
      const createSQL = this.buildCreateSQLFor(tempName);
      this.InternalDBRefrance.exec(createSQL);

      // Build column copy mapping
      const targetCols: string[] = [];
      const selectExprs: string[] = [];
      for (const newCol of desiredColumns) {
        targetCols.push(`"${newCol}"`);
        const mappedOld = newToOld.get(newCol) ?? newCol;
        if (existingNames.has(mappedOld)) {
          selectExprs.push(`"${mappedOld}"`);
        } else {
          const field = (this.Schema as any)[newCol];
          const literal = this.defaultLiteralForField(field);
          selectExprs.push(`${literal} AS "${newCol}"`);
        }
      }

      if (existing.length > 0) {
        const insertSQL = `INSERT INTO "${tempName}" (${targetCols.join(", ")}) SELECT ${selectExprs.join(", ")} FROM "${this.Name}"`;
        this.InternalDBRefrance.exec(insertSQL);
      }

      // Replace old table
      this.InternalDBRefrance.exec(`DROP TABLE IF EXISTS "${this.Name}"`);
      this.InternalDBRefrance.exec(`ALTER TABLE "${tempName}" RENAME TO "${this.Name}"`);

      this.InternalDBRefrance.run("PRAGMA foreign_keys = ON");
      this.InternalDBRefrance.run("COMMIT");
    } catch (e) {
      try { this.InternalDBRefrance.run("ROLLBACK"); } catch {}
      try { this.InternalDBRefrance.run("PRAGMA foreign_keys = ON"); } catch {}
      throw e;
    }
  };
}
