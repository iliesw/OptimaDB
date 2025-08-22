import { Database } from "bun:sqlite";
import { OptimaDB } from "./database";
import {
  applyFormatIn,
  applyFormatOut,
  ExtendTables,
  FieldReferenceMany,
  FieldToSQL,
  isFieldInsertOptional,
  OptimaField,
  OptimaTable,
  TableReferencesTableByMany,
  TableToSQL,
} from "./schema";
import { EventEmitter } from "events";

type GetType<
  T extends OptimaTable<any>,
  K extends ExtendTables<T, S> | Array<ExtendTables<T, S>>,
  S extends Record<string, OptimaTable<any>>
> = {
  [P in keyof T as P extends "__tableName"
    ? never
    : P]: T[P] extends OptimaField<any, any, any>
    ? OptimaFieldToTS<T[P]>
    : never;
} & (K extends string
  ? {
      [Key in K as `$${Extract<Key, string>}`]: TableReferencesTableByMany<
        S[K],
        T
      > extends true
        ? RowOf<S[K]>[]
        : RowOf<S[K]>;
    }
  : K extends readonly (keyof S)[]
  ? {
      [Key in K[number] as `$${Extract<
        Key,
        string
      >}`]: TableReferencesTableByMany<S[Key], T> extends true
        ? RowOf<S[Key]>[]
        : RowOf<S[Key]>;
    }
  : {});

type OptimaFieldToTS<F extends OptimaField<any, any, any>> =
  F extends OptimaField<infer K, any, any> ? K : never;

export type ColumnWhere<T> = T | null | T[] | WhereOperatorObject<T>;
export type BasicWhere<TDef extends OptimaTable<Record<string, any>>> = {
  [K in keyof TDef as K extends "__tableName" ? never : K]?: ColumnWhere<OptimaFieldToTS<TDef[K]>>;
};
export type WhereInput<TDef extends OptimaTable<Record<string, any>>> =
  | BasicWhere<TDef> & {
      $or?: WhereInput<TDef>[];
      $and?: WhereInput<TDef>[];
    };
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
export type UpdateChanges<T extends OptimaTable<Record<string, any>>> =
  Partial<{
    [K in keyof T as K extends "__tableName" ? never : K]: OptimaFieldToTS<T[K]>;
  }>;

export type RowOf<TDef extends OptimaTable<Record<string, any>>> = {
  [K in keyof TDef as K extends "__tableName" ? never : K]: OptimaFieldToTS<
    TDef[K]
  >;
};
export type InsertInput<
  TDef extends OptimaTable<Record<string, OptimaField<any, any, any>>>
> =
  // Required keys
  {
    [K in keyof TDef as K extends "__tableName"
      ? never
      : isFieldInsertOptional<TDef[K]> extends true
      ? never
      : K]: OptimaFieldToTS<TDef[K]>;
  } & { // Optional keys
    [K in keyof TDef as K extends "__tableName"
      ? never
      : isFieldInsertOptional<TDef[K]> extends true
      ? K
      : never]?: OptimaFieldToTS<TDef[K]>;
  };

export class OptimaTB<
  T extends OptimaTable<Record<string, any>>,
  S extends Record<string, OptimaTable<any>>,
  N extends string = string
> {
  private Name: N;
  private InternalDBReference: Database;
  private InternalOptimaDBReference: OptimaDB<S>;
  private isHybrid: boolean;
  private Schema: T;
  private SchemaRef: S;
  private ChangeEvent?: EventEmitter;
  private ChangeConfig = {
    ChangeCounter: 0,
    Threshold: 2000,
    Timer: 5000,
    LastSave: 0,
  };
  private extendRelationships: Map<
    string,
    {
      ExternalField: string;
      InternalField: string;
      Type: "One" | "Many";
    }
  > = new Map();

  private InitTable(Tables: S) {
    this.InternalDBReference.query(TableToSQL(this.Schema, this.Name)).run();
    for (const tableName of Object.keys(Tables)) {
      if (tableName === this.Name) continue;
      const tableSchema = (Tables as Record<string, any>)[tableName];
      const cols = tableSchema;
      delete cols["__tableName"];
      for (const columnName of Object.keys(cols)) {
        const columnSchema = cols[columnName];
        if (
          columnSchema?.Reference &&
          columnSchema.Reference.TableName === this.Name
        ) {
          this.extendRelationships.set(tableName, {
            ExternalField: columnName,
            InternalField: columnSchema.Reference.Field,
            Type: columnSchema.Reference.Type,
          });
        }
      }
    }
  }

  constructor(
    InternalDB: Database,
    TableName: N,
    SchemaTable: T,
    TablesToCompute: S,
    OptimaDBRef: OptimaDB<S>,
    isHybrid: boolean
  ) {
    this.InternalDBReference = InternalDB;
    this.InternalOptimaDBReference = OptimaDBRef;
    this.Schema = SchemaTable;
    this.SchemaRef = TablesToCompute;
    this.Name = TableName;
    this.isHybrid = isHybrid;
    this.InitTable(TablesToCompute);
    if (isHybrid) {
      this.ChangeEvent = new EventEmitter();
      this.ChangeConfig.LastSave = Date.now();
      this.ChangeEvent.on("Change", () => {
        this.ChangeConfig.ChangeCounter++;
        const Now = Date.now();
        const LastSave = this.ChangeConfig.LastSave;
        const Difference = Now - LastSave;

        if (
          Difference >= this.ChangeConfig.Timer ||
          this.ChangeConfig.ChangeCounter >= this.ChangeConfig.Threshold
        ) {
          (this.InternalOptimaDBReference as any)["SaveToDisk"]();
          this.ChangeConfig.ChangeCounter = 0;
          this.ChangeConfig.LastSave = Date.now();
        }
      });
    }
  }

  Get = <Ext extends ExtendTables<T, S> | Array<ExtendTables<T, S>>>(
    where?: WhereInput<T>,
    options?: {
      Limit?: number;
      Offset?: number;
      Unique?: boolean;
      Extend?: Ext;
      OrderBy?: {
        Column: keyof T & string;
        Direction: "ASC" | "DESC";
      };
    }
  ): GetType<T, Ext, S>[] => {
    const { clause, params } = buildWhereClause(this, where as any);
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
      limitClause += " LIMIT -1 OFFSET ?";
      extraParams.push(Number(options.Offset));
    }

    const sql = `${selectPrefix} FROM "${this.Name}"${clause}${orderClause}${limitClause}`;
    const rows = this.InternalDBReference.query(sql).all(
      ...params,
      ...extraParams
    );
    let cleanRows = rows.map((r: any) => mapOutRow(this, r));

    if (options?.Extend != undefined) {
      const extendArray = Array.isArray(options.Extend)
        ? options.Extend
        : [options.Extend];
      extendArray.forEach((e) => {
        if (this.extendRelationships.has(e)) {
          cleanRows = cleanRows.map((r: any) => {
            const Table = e as string;
            const Relation = this.extendRelationships.get(Table)!;
            let Data;

            if (Relation.Type === "Many") {
              Data = (this.InternalOptimaDBReference.Tables as any)[Table]?.Get(
                {
                  [Relation.ExternalField]: r[Relation.InternalField],
                }
              );
            } else {
              Data = (this.InternalOptimaDBReference.Tables as any)[
                Table
              ]?.GetOne({
                [Relation.ExternalField]: r[Relation.InternalField],
              });
            }
            return { ...r, [`$${e}`]: Data };
          });
        } else {
          throw new Error(
            `Table ${this.Name} doesn't have a relation to the table ${e}`
          );
        }
      });
    }

    return cleanRows as GetType<T, Ext, S>[];
  };

  GetOne = <Ext extends ExtendTables<T, S> | Array<ExtendTables<T, S>>>(
    where?: WhereInput<T>,
    options?: {
      Extend?: Ext;
    }
  ): GetType<T, Ext, S> | undefined => {
    const { clause, params } = buildWhereClause(this, where as any);
    const row = this.InternalDBReference.query(
      `SELECT * FROM "${this.Name}"${clause}`
    ).get(...params);
    let mappedRow = mapOutRow(this, row);

    if (!mappedRow) return mappedRow;

    if (options?.Extend != undefined) {
      const extendArray = Array.isArray(options.Extend)
        ? options.Extend
        : [options.Extend];
      extendArray.forEach((e) => {
        if (this.extendRelationships.has(e)) {
          const Table = e as string;
          const Relation = this.extendRelationships.get(Table)!;
          let Data;

          if (Relation.Type === "Many") {
            Data = (this.InternalOptimaDBReference.Tables as any)[Table]?.Get({
              [Relation.ExternalField]: mappedRow[Relation.InternalField],
            });
          } else {
            Data = (this.InternalOptimaDBReference.Tables as any)[
              Table
            ]?.GetOne({
              [Relation.ExternalField]: mappedRow[Relation.InternalField],
            });
          }
          mappedRow = { ...mappedRow, [`$${e}`]: Data };
        } else {
          throw new Error(
            `Table ${this.Name} doesn't have a relation to the table ${e}`
          );
        }
      });
    }

    return mappedRow as GetType<T, Ext, S>;
  };

  Insert = (Values: InsertInput<T>) => {
    // Handle both new Table() format and direct schema format
    const cols = this.Schema;
    // Runtime validation: ensure all NOT NULL fields are present and non-null
    for (const key of Object.keys(cols)) {
      const field = cols[key] as OptimaField<any, any, any>;
      const valueProvided = Object.prototype.hasOwnProperty.call(Values, key);
      const notNull = field["NotNull"];

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

    const columns = Object.keys(Values as unknown as Record<string, unknown>);
    const placeholders = columns.map(() => "?").join(", ");
    const sql = `INSERT INTO "${this.Name}" (${columns
      .map((col) => `"${col}"`)
      .join(", ")}) VALUES (${placeholders})`;
    const stmt = this.InternalDBReference.prepare(sql);

    const formattedValues = columns.map((col) => {
      const field = cols[col];
      const raw = (Values as any)[col];
      if (field) {
        return applyFormatIn(field, raw);
      }
      return raw;
    });
    const res = stmt.run(...formattedValues);

    if (this.isHybrid && this.ChangeEvent) {
      this.ChangeEvent.emit("Change");
    }

    return res;
  };

  InsertMany = (Values: InsertInput<T>[]) => {
    this.InternalOptimaDBReference.Batch(() => {
      for (const row of Values) {
        this.Insert(row);
      }
    });
  };

  Update = (values: UpdateChanges<T>, where?: WhereInput<T>) => {
    // For updates, if a NOT NULL field is explicitly set to null, block it
    for (const key of Object.keys(values)) {
      const field = (this.Schema as any)[key] as OptimaField<any, any, any> & {
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
        field
          ? applyFormatIn(field,raw)
          : raw;
      setParts.push(`"${col}" = ?`);
      setParams.push(formatted);
    }

    const whereBuilt = buildWhereClause(this, where as any);
    const sql = `UPDATE "${this.Name}" SET ${setParts.join(", ")}${
      whereBuilt.clause
    }`;
    const stmt = this.InternalDBReference.prepare(sql);
    const res = stmt.run(...setParams, ...whereBuilt.params);
    if (this.isHybrid) {
      this.ChangeEvent.emit("Change");
    }
    return res;
  };

  Delete = (where?: WhereInput<T>) => {
    const whereBuilt = buildWhereClause(this, where as any);
    const sql = `DELETE FROM "${this.Name}"${whereBuilt.clause}`;
    const stmt = this.InternalDBReference.prepare(sql);
    const res = stmt.run(...whereBuilt.params);
    if (this.isHybrid) {
      this.ChangeEvent.emit("Change");
    }
    return res;
  };

  Count = (where?: WhereInput<T>) => {
    const { clause, params } = buildWhereClause(this, where as any);
    const row = this.InternalDBReference.query(
      `SELECT COUNT(*) as count FROM "${this.Name}"${clause}`
    ).get(...params) as { count: number };
    return row ? row.count : 0;
  };
}

const mapOutRow = (table: OptimaTB<any, any>, row: any) => {
  if (!row) return row;
  const result: Record<string, any> = {};
  const cols = (table["Schema"] as any).cols || table["Schema"];
  for (const key of Object.keys(row)) {
    const field = cols[key];
    if (field) {
      result[key] = applyFormatOut(field, row[key]);
    } else {
      result[key] = row[key];
    }
  }
  return result;
};
const buildWhereClause = (table: OptimaTB<any, any>, where?: any) => {
  if (
    !where ||
    (typeof where === "object" && Object.keys(where).length === 0)
  ) {
    return { clause: "", params: [] as any[] } as const;
  }

  if (typeof where === "string") {
    return { clause: ` WHERE ${where}`, params: [] as any[] } as const;
  }

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

    // Handle both new Table() format and direct schema format
    const cols = (table["Schema"] as any).cols || table["Schema"];

    const ensureFormatted = (column: string, value: any) => {
      const field = cols[column];
      if (field) {
        return applyFormatIn(field, value);
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
          if (
            value !== null &&
            typeof value === "object" &&
            !Array.isArray(value)
          ) {
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
const tableExists = (table: OptimaTB<any, any>): boolean => {
  const row = table["InternalDBReference"]
    .query("SELECT name FROM sqlite_master WHERE type='table' AND name = ?")
    .get(table["Name"]) as { name?: string } | undefined;
  return !!row && row.name === table["Name"];
};
const getExistingColumns = (table: OptimaTB<any, any>) => {
  if (!tableExists(table))
    return [] as Array<{
      name: string;
      type: string | null;
      notnull: number;
      dflt_value: any;
      pk: number;
    }>;
  const rows = table["InternalDBReference"]
    .query(`PRAGMA table_info("${table["Name"]}")`)
    .all() as Array<{
    cid: number;
    name: string;
    type: string | null;
    notnull: number;
    dflt_value: any;
    pk: number;
  }>;
  return rows;
};
const buildCreateSQLFor = (name: string, table: OptimaTB<any, any>): string => {
  const colDefs = Object.entries(table["Schema"]).map(
    ([colName, field]) => {
      // Access the internal SQL builder the same way Table() does
      const def = FieldToSQL(field as OptimaField<any,any,any>);
      return `"${colName}" ${def}`;
    }
  );
  return `CREATE TABLE "${name}" (\n  ${colDefs.join(",\n  ")}\n);`;
};
const defaultLiteralForField = (field: any): string => {
  try {
    const hasDefault = field["Default"] != null;

    if (!hasDefault) return "NULL";
    const rawDefault = field["Default"];
    const formatted = applyFormatIn(field, rawDefault);
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
export const MigrateSchema = (
  table: OptimaTB<any, any>,
  renameColumns?: Record<string, string>
) => {
  const existing = getExistingColumns(table);
  const desiredColumns = Object.keys(table["Schema"]);

  // If table doesn't exist yet, just create it using the normal initializer and return.
  if (!tableExists(table)) {
    table["InternalDBReference"].exec(buildCreateSQLFor(table["Name"], table));
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
  for (const n of desiredNameSet)
    if (!normalizedExistingNames.has(n)) requiresRebuild = true;
  for (const n of normalizedExistingNames)
    if (!desiredNameSet.has(n)) requiresRebuild = true;

  // If sets match, we could still differ on constraints/types; rebuild to be safe.
  // To avoid unnecessary churn, you could diff types via PRAGMA table_info, but we prefer safety here.
  // If you want to skip in that case, comment the next line.
  // Keep rebuild on by default to ensure constraints are applied.
  requiresRebuild = requiresRebuild || true;

  if (!requiresRebuild) return;

  const tempName = `__tmp__${table["Name"]}`;

  table["InternalDBReference"].run("BEGIN");
  try {
    table["InternalDBReference"].run("PRAGMA foreign_keys = OFF");

    // Create temp table with desired schema
    const createSQL = buildCreateSQLFor(tempName, table);
    table["InternalDBReference"].exec(createSQL);

    // Build column copy mapping
    const targetCols: string[] = [];
    const selectExprs: string[] = [];
    for (const newCol of desiredColumns) {
      targetCols.push(`"${newCol}"`);
      const mappedOld = newToOld.get(newCol) ?? newCol;
      if (existingNames.has(mappedOld)) {
        selectExprs.push(`"${mappedOld}"`);
      } else {
        const field = (table["Schema"] as any)[newCol];
        const literal = defaultLiteralForField(field);
        selectExprs.push(`${literal} AS "${newCol}"`);
      }
    }

    if (existing.length > 0) {
      const insertSQL = `INSERT INTO "${tempName}" (${targetCols.join(
        ", "
      )}) SELECT ${selectExprs.join(", ")} FROM "${table["Name"]}"`;
      table["InternalDBReference"].exec(insertSQL);
    }

    // Replace old table
    table["InternalDBReference"].exec(
      `DROP TABLE IF EXISTS "${table["Name"]}"`
    );
    table["InternalDBReference"].exec(
      `ALTER TABLE "${tempName}" RENAME TO "${table["Name"]}"`
    );
    table["InternalDBReference"].run("PRAGMA foreign_keys = ON");
    table["InternalDBReference"].run("COMMIT");
  } catch (e) {
    try {
      table["InternalDBReference"].run("ROLLBACK");
    } catch {}
    try {
      table["InternalDBReference"].run("PRAGMA foreign_keys = ON");
    } catch {}
    throw e;
  }
};
