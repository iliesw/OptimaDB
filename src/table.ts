import { Database } from "./driver";
import { OptimaDB } from "./database";
import {
  applyFormatIn,
  applyFormatOut,
  buildFormatter,
  ExtendTables,
  FieldTypes,
  GetType,
  InsertInput,
  OptimaTable,
  TableToSQL,
  TypeChecker,
  UpdateChanges,
  WhereInput,
} from "./schema";
import { EventEmitter } from "events";
import { BuildCond } from "./builder";

export class OptimaTB<
  T extends OptimaTable<Record<string, any>>,
  S extends Record<string, OptimaTable<any>>,
  N extends string = string
> {
  private Name: N;
  private InternalDBReference: typeof Database;
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
  private InsertQCache = new Map();
  private SelectQCache = new Map();
  private UpdateQCache = new Map();
  private DeleteQCache = new Map();
  private compiledSchema: {
    key: string;
    type: FieldTypes;
    notNull: boolean;
    check?: (val: any) => boolean;
    isPassword: boolean;
    formatter: (val: any) => any;
  }[] = [];

  constructor(
    InternalDB: typeof Database,
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
  private InitTable(Tables: S) {
    this.InternalDBReference.query(TableToSQL(this.Schema, this.Name)).run();
    MigrateTable(this, this.Schema);
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
    this.CompileInsertQuerries();
    this.CompileSchema();
    this.CompileUpdateQuerries();
    this.CompileDeleteQuerries();
  }
  private CompileSchema() {
    this.compiledSchema = Object.entries(this.Schema).map(([key, field]) => ({
      key,
      type: field["Type"],
      notNull: !!field["NotNull"],
      check: field["Check"],
      isPassword: field["Type"] === FieldTypes.Password,
      formatter: buildFormatter(field["Type"]),
    }));
  }
  private CompileInsertQuerries() {
    const InsertQ = new Map();
    const Keys = Object.keys(this.Schema);

    // Helper: generate all non-empty subsets
    const subsets = (arr: string[]) => {
      const result: string[][] = [];
      const n = arr.length;
      for (let mask = 1; mask < 1 << n; mask++) {
        const subset: string[] = [];
        for (let i = 0; i < n; i++) {
          if (mask & (1 << i)) subset.push(arr[i]);
        }
        result.push(subset);
      }
      return result;
    };

    const allCombos = subsets(Keys);

    allCombos.forEach((cols) => {
      const placeholders = cols.map(() => "?").join(", ");
      const sql = `INSERT INTO "${this.Name}" (${cols
        .map((c) => `"${c}"`)
        .join(", ")}) VALUES (${placeholders})`;

      const key = cols.sort().join(","); // Sort to ensure consistent key ordering
      InsertQ.set(key, this.InternalDBReference.prepare(sql));
      InsertQ.set(
        key + "-R",
        this.InternalDBReference.prepare(sql + " Returning *")
      );
    });

    this.InsertQCache = InsertQ;
  }
  private CompileUpdateQuerries() {
    // Prepare and cache update statements for all possible column combinations
    // Key: columns sorted, joined by ',' + where string + (Returning ? "-R" : "")
    this.UpdateQCache = new Map();
    const Keys = Object.keys(this.Schema);

    // Helper: generate all non-empty subsets
    const subsets = (arr: string[]) => {
      const result: string[][] = [];
      const n = arr.length;
      for (let mask = 1; mask < 1 << n; mask++) {
        const subset: string[] = [];
        for (let i = 0; i < n; i++) {
          if (mask & (1 << i)) subset.push(arr[i]);
        }
        result.push(subset);
      }
      return result;
    };

    const allCombos = subsets(Keys);

    // We can't cache for all possible WHEREs, but we can cache for all possible SETs
    allCombos.forEach((cols) => {
      // We'll use a placeholder for WHERE, and replace it at runtime
      const setParts = cols.map((col) => `"${col}" = ?`).join(", ");
      // We'll cache the SET part, and at runtime, append the WHERE and RETURNING
      const key = cols.sort().join(",");
      this.UpdateQCache.set(key, setParts);
    });
  }
  private CompileDeleteQuerries() {
    // Cache delete statements for each unique WHERE clause (stringified)
    // Key: whereBuilt + (Returning ? "-R" : "")
    this.DeleteQCache = new Map();
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
    let clause = "";
    let orderClause = "";
    let limitClause = "";
    const extraParams: any[] = [];
    const selectPrefix = options?.Unique ? "SELECT DISTINCT *" : "SELECT *";
    if (where) {
      const tempClause = BuildCond(where, this.Schema);
      clause = tempClause == "" ? "" : "WHERE " + tempClause;
    }
    if (options?.OrderBy) {
      orderClause = ` ORDER BY "${options.OrderBy.Column}" ${options.OrderBy.Direction}`;
    }
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

    const sql = `${selectPrefix} FROM "${this.Name}" ${clause} ${orderClause} ${limitClause}`;
    console.log(sql)
    let stmt = this.SelectQCache.get(sql);
    if (!stmt) {
      stmt = this.InternalDBReference.query(sql);
      this.SelectQCache.set(sql, stmt);
    }
    const rows = stmt.all(...extraParams);
    let cleanRows = rows.map((r: any) => this.mapOutRow(r));
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
    const clause = BuildCond(where != undefined ? where : {}, this.Schema);
    const sql = `SELECT * FROM "${this.Name}" ${
      clause == "" ? "" : " WHERE " + clause
    } LIMIT 1`;
    let stmt = this.SelectQCache.get(sql);

    if (!stmt) {
      stmt = this.InternalDBReference.query(sql);
      this.SelectQCache.set(sql, stmt);
    }
    const row = stmt.get();
    let mappedRow = this.mapOutRow(row);

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
  Insert = (
    Values: InsertInput<T>,
    Returning?: boolean
  ): GetType<T, null, S> => {
    let formattedValues = [];
    for (const f of this.compiledSchema) {
      let val = Values[f.key];
      const provided = Object.prototype.hasOwnProperty.call(Values, f.key);
      if (f.notNull) {
        if (!provided || val === null || val === undefined) {
          throw new Error(
            `Field "${f.key}" is NOT NULL and must be provided in ${this.Name}`
          );
        }
      }
      // Type & check validation
      if (provided) {
        if (!TypeChecker(val, f.type)) {
          throw new Error(`"${val}" is not a valid ${f.type}`);
        }
        if (f.check && !f.check(val)) {
          throw new Error(`"${val}" failed custom check in field "${f.key}"`);
        }
      }
      formattedValues.push(applyFormatIn(this.Schema[f.key], val));
    }
    const cacheKey =
      Object.keys(Values).sort().join(",") + (Returning ? "-R" : "");
    const stmt = this.InsertQCache.get(cacheKey);
    if (!stmt) {
      throw new Error(
        `No prepared statement found for columns: ${Object.keys(Values).join(
          ", "
        )}. This usually indicates a schema mismatch.`
      );
    }

    const res =
      Returning != undefined && Returning == true
        ? stmt.get(
            ...formattedValues.filter((e) => {
              return e != undefined;
            })
          )
        : stmt.run(
            ...formattedValues.filter((e) => {
              return e != undefined;
            })
          );

    if (this.isHybrid && this.ChangeEvent) {
      this.ChangeEvent.emit("Change");
    }

    return Returning != undefined && Returning == true
      ? this.mapOutRow(res)
      : res;
  };
  InsertMany = (
    Values: InsertInput<T>[],
    Returning?: boolean
  ): GetType<T, null, S>[] => {
    const Res = [];
    this.InternalOptimaDBReference.Batch(() => {
      for (const row of Values) {
        Res.push(this.Insert(row, Returning));
      }
    });
    return Res;
  };
  Update = (
    values: UpdateChanges<T>,
    where?: WhereInput<T>,
    Returning?: boolean
  ): GetType<T, null, S>[] => {
    for (const f of this.compiledSchema) {
      if (Object.prototype.hasOwnProperty.call(values, f.key)) {
        const val = (values as any)[f.key];
        if (f.notNull && val === null) {
          throw new Error(
            `Field "${f.key}" is NOT NULL and cannot be set to null in ${this.Name}`
          );
        }
        if (!TypeChecker(val, f.type)) {
          throw new Error(`"${val}" is not a valid ${f.type}`);
        }
        if (f.check && !f.check(val)) {
          throw new Error(`"${val}" failed custom check in field "${f.key}"`);
        }
      }
    }

    const columns = Object.keys(values);
    if (columns.length === 0) return { changes: 0 } as any;

    const formattedValues = columns.map((col) => {
      const field = this.Schema[col];
      return field ? applyFormatIn(field, values[col]) : values[col];
    });

    const sortedCols = columns.slice().sort();
    const setKey = sortedCols.join(",");
    const setParts = this.UpdateQCache.get(setKey);
    if (!setParts) {
      throw new Error(
        `No prepared SET clause found for columns: ${columns.join(
          ", "
        )}. This usually indicates a schema mismatch.`
      );
    }

    const whereBuilt = BuildCond(where ?? {}, this.Schema);

    const sql =
      `UPDATE "${this.Name}" SET ${setParts}` +
      ` WHERE ${whereBuilt}` +
      (Returning ? " RETURNING *" : "");
    const stmtCacheKey = setKey + "|" + whereBuilt + (Returning ? "-R" : "");
    let stmt = this.UpdateQCache.get(stmtCacheKey);
    if (!stmt) {
      stmt = this.InternalDBReference.prepare(sql);
      this.UpdateQCache.set(stmtCacheKey, stmt);
    }

    const res = Returning
      ? stmt.all(
          ...formattedValues.filter((e) => {
            return e != undefined;
          })
        )
      : stmt.run(
          ...formattedValues.filter((e) => {
            return e != undefined;
          })
        );

    if (this.isHybrid && this.ChangeEvent) {
      this.ChangeEvent.emit("Change");
    }

    return Returning
      ? Array.isArray(res)
        ? res.map((e: any) => this.mapOutRow(e))
        : []
      : res;
  };
  Delete = (where?: WhereInput<T>, Returning?: boolean) => {
    const whereBuilt = BuildCond(where != undefined ? where : {}, this.Schema);
    const cacheKey = whereBuilt + (Returning ? "-R" : "");
    let stmt = this.DeleteQCache.get(cacheKey);

    // Compose SQL
    const sql =
      `DELETE FROM "${this.Name}" WHERE ${whereBuilt}` +
      (Returning ? " RETURNING *" : "");

    if (!stmt) {
      stmt = this.InternalDBReference.prepare(sql);
      this.DeleteQCache.set(cacheKey, stmt);
    }

    let res = Returning ? stmt.all() : stmt.run();

    if (this.isHybrid && this.ChangeEvent) {
      this.ChangeEvent.emit("Change");
    }

    return Returning
      ? Array.isArray(res)
        ? res.map((e: any) => this.mapOutRow(e))
        : []
      : res;
  };
  Count = (where?: WhereInput<T>) => {
    const clause = BuildCond(where != undefined ? where : {}, this.Schema);
    const row = this.InternalDBReference.query(
      `SELECT COUNT(*) as count FROM "${this.Name}" ${
        clause != "" ? "WHERE " + clause : ""
      }`
    ).get() as { count: number };
    return row ? row.count : 0;
  };
  private mapOutRow = (row: any) => {
    if (!row) return row;
    const result: Record<string, any> = {};
    const cols = this.Schema;
    for (const key of Object.keys(row)) {
      result[key] = applyFormatOut(cols[key]["Type"], row[key]);
    }
    return result;
  };
}

export const MigrateTable = (
  Table: OptimaTB<any, any>,
  Schema: OptimaTable<Record<string, any>>
) => {
  const pragmaStmt = `PRAGMA table_info("${Table["Name"]}")`;
  const OldSchema = Table["InternalDBReference"]
    .query(pragmaStmt)
    .all()
    .map((col: any) => {
      return {
        Name: col.name,
        Type: col.type,
        NotNull: col.notnull == 1,
        PrimaryKey: col.pk == 1,
        Default: col.dflt_value,
      };
    });
  const NewKeys = Schema;
  // Find new columns (in NewKeys but not in OldSchema)
  const oldNames = OldSchema.map((col) => col.Name);
  const newNames = Object.keys(NewKeys);

  const Conflicts = {
    New: newNames.filter((name) => !oldNames.includes(name)),
    Updated: newNames.filter((name) => {
      const oldCol = OldSchema.find((col) => col.Name === name);
      if (!oldCol) return false;
      const newCol = NewKeys[name];
      const typeChanged =
        (oldCol.Type || "").toLowerCase() !== (newCol.Type || "").toLowerCase();
      const notNullChanged = !!oldCol.NotNull !== !!newCol.NotNull;
      const pkChanged = !!oldCol.PrimaryKey !== !!newCol.PrimaryKey;
      let defaultChanged = false;
      if (oldCol.Default === null || oldCol.Default === undefined) {
        defaultChanged =
          newCol.Default !== undefined && newCol.Default !== null;
      } else {
        defaultChanged = String(oldCol.Default) !== String(newCol.Default);
      }
      return typeChanged || notNullChanged || pkChanged; //|| defaultChanged;
    }),
    Deleted: oldNames.filter((name) => !newNames.includes(name)),
  };
  // Solve Conflicts
  // If there are no changes, return early
  if (
    Conflicts.New.length === 0 &&
    Conflicts.Updated.length === 0 &&
    Conflicts.Deleted.length === 0
  ) {
    return;
  }

  // 1. Get all rows from the old table
  const allRows = Table["InternalDBReference"]
    .query(`SELECT * FROM "${Table["Name"]}"`)
    .all();

  // 2. Build new schema SQL
  //    - Only include columns that are in the new schema (ignore deleted)
  //    - Use FieldToSQL to get column definitions
  const { FieldToSQL } = require("./schema");
  const newCols = Object.keys(NewKeys);
  const colDefs = newCols.map((col) => {
    return `"${col}" ${FieldToSQL(NewKeys[col])}`;
  });
  const tempTableName = `__tmp_${Table["Name"]}_${Date.now()}`;

  // 3. Create a new temp table with the new schema
  Table["InternalDBReference"].exec(
    `CREATE TABLE "${tempTableName}" (${colDefs.join(", ")})`
  );

  // 4. Insert old data into the new table
  for (const row of allRows) {
    const insertCols = [];
    const insertVals = [];
    for (const col of newCols) {
      if (row.hasOwnProperty(col)) {
        insertCols.push(`"${col}"`);
        insertVals.push(row[col]);
      } else {
        // New column: use default if available, else null
        if (
          NewKeys[col] &&
          Object.prototype.hasOwnProperty.call(NewKeys[col], "Default")
        ) {
          insertCols.push(`"${col}"`);
          insertVals.push(NewKeys[col].Default);
        } else {
          insertCols.push(`"${col}"`);
          insertVals.push(null);
        }
      }
    }
    const placeholders = insertVals.map(() => "?").join(", ");
    Table["InternalDBReference"]
      .prepare(
        `INSERT INTO "${tempTableName}" (${insertCols.join(
          ", "
        )}) VALUES (${placeholders})`
      )
      .run(insertVals);
  }

  // 5. Drop the old table
  Table["InternalDBReference"].exec(`DROP TABLE "${Table["Name"]}"`);

  // 6. Rename the temp table to the original name
  Table["InternalDBReference"].exec(
    `ALTER TABLE "${tempTableName}" RENAME TO "${Table["Name"]}"`
  );
};
