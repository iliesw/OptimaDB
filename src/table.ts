import { Database } from "bun:sqlite";
import { OptimaDB } from "./database";
import {
  applyFormatIn,
  applyFormatOut,
  buildFormatter,
  ExtendTables,
  FieldReferenceMany,
  FieldToSQL,
  FieldTypes,
  GetType,
  InsertInput,
  OptimaField,
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
  private InsertQCache = new Map();
  private SelectQCache = new Map();
  private compiledSchema: {
    key: string;
    type: FieldTypes;
    notNull: boolean;
    check?: (val: any) => boolean;
    isPassword: boolean;
    formatter: (val: any) => any;
  }[] = [];

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
    this.CompileInsertQuerries();
    this.CompileSchema();
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
  Upsert = (Values: InsertInput<T>): GetType<T, null, S> => {
    const cols = this.Schema;

    // Runtime validation: ensure all NOT NULL fields are present and non-null
    for (const key of Object.keys(cols)) {
      const field = cols[key] as OptimaField<any, any, any>;
      const valueProvided = Object.prototype.hasOwnProperty.call(Values, key);
      const notNull = field["NotNull"];

      if (
        field["Type"] == FieldTypes.UUID &&
        field["Default"] == undefined &&
        valueProvided == false
      ) {
        Values[key] = Bun.randomUUIDv7();
      }
      if (field["Type"] == FieldTypes.Password && valueProvided) {
        Values[key] = Bun.password.hashSync(Values[key], "bcrypt");
      }
      if (
        field["Type"] == FieldTypes.Password &&
        !valueProvided &&
        !field["NotNull"]
      ) {
        Values[key] = null;
      }
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

    // TypeChecker
    for (const [key, val] of Object.entries(Values)) {
      const field = cols[key] as OptimaField<any, any, any>;
      const fieldType = field["Type"];
      const isValid = TypeChecker(val, fieldType);
      if (!isValid && val) {
        throw new Error("`" + val + "` is not a valid " + fieldType);
      }
      if (field["Check"] != undefined) {
        const checkPass = field["Check"](val);
        if (!checkPass) {
          throw new Error(
            "`" + val + "` failed to satisfy the check function in field " + key
          );
        }
      }
    }

    const columns = Object.keys(Values as unknown as Record<string, unknown>);
    const placeholders = columns.map(() => "?").join(", ");

    // pick conflict target (prefer primary key if defined, otherwise unique index)
    const conflictCols = Object.keys(cols).filter(
      (k) => (cols[k] as any)["PrimaryKey"] || (cols[k] as any)["Unique"]
    );
    if (conflictCols.length === 0) {
      throw new Error(
        `No PRIMARY KEY or UNIQUE constraint found in ${this.Name}, UPSERT not possible`
      );
    }

    // generate DO UPDATE SET col = excluded.col for all updatable fields
    const updateSet = columns
      .filter((col) => !conflictCols.includes(col)) // don’t overwrite PK
      .map((col) => `"${col}" = excluded."${col}"`)
      .join(", ");

    const sql = `
      INSERT INTO "${this.Name}" (${columns.map((c) => `"${c}"`).join(", ")})
      VALUES (${placeholders})
      ON CONFLICT (${conflictCols.map((c) => `"${c}"`).join(", ")})
      DO UPDATE SET ${updateSet}
      RETURNING *;
    `;

    const stmt = this.InternalDBReference.prepare(sql);

    const formattedValues = columns.map((col) => {
      const field = cols[col];
      const raw = (Values as any)[col];
      if (field) {
        return applyFormatIn(field, raw);
      }
      return raw;
    });
    const res = stmt.get(...formattedValues);

    if (this.isHybrid && this.ChangeEvent) {
      this.ChangeEvent.emit("Change");
    }

    return this.mapOutRow(res);
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
      if (this.Schema[f.key]["Type"] == FieldTypes.Password && provided) {
        Values[f.key] = Bun.password.hashSync(Values[f.key], "bcrypt");
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
    where?: WhereInput<T>
  ): GetType<T, null, S>[] => {
    const cols = this.Schema;

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

    // Type checking and validation (same as Insert method)
    for (const [key, val] of Object.entries(values)) {
      const field = cols[key] as OptimaField<any, any, any>;
      if (!field) continue; // Skip if field doesn't exist in schema

      // Handle password field hashing
      if (
        field["Type"] == FieldTypes.Password &&
        val !== null &&
        val !== undefined
      ) {
        (values as any)[key] = Bun.password.hashSync(val, "bcrypt");
      }

      // Type checking
      const fieldType = field["Type"];
      const isValid = TypeChecker(val, fieldType);
      if (!isValid) {
        throw new Error("`" + val + "` is not a valid " + fieldType);
      }

      // Check function validation
      if (field["Check"] != undefined) {
        const checkPass = field["Check"](val);
        if (!checkPass) {
          throw new Error(
            "`" + val + "` failed to satisfy the check function in field " + key
          );
        }
      }
    }

    const columns = Object.keys(values);
    if (columns.length === 0) return { changes: 0 } as any;

    const setParts: string[] = [];
    const setParams: any[] = [];
    for (const col of columns) {
      const field = (this.Schema as any)[col];
      const raw = values[col];
      const formatted = field ? applyFormatIn(field, raw) : raw;
      setParts.push(`"${col}" = ?`);
      setParams.push(formatted);
    }

    const whereBuilt = BuildCond(where != undefined ? where : {}, this.Schema);
    const sql = `UPDATE "${this.Name}" SET ${setParts.join(
      ", "
    )}${whereBuilt} RETURNING *`;
    const stmt = this.InternalDBReference.prepare(sql);
    const res = stmt.all(...setParams);
    if (this.isHybrid) {
      this.ChangeEvent.emit("Change");
    }
    return res.map((e: any) => {
      return this.mapOutRow(e);
    });
  };
  Delete = (where?: WhereInput<T>) => {
    const whereBuilt = BuildCond(where != undefined ? where : {}, this.Schema);
    const sql = `DELETE FROM "${this.Name}"${whereBuilt} RETURNING *`;
    const stmt = this.InternalDBReference.prepare(sql);
    const res = stmt.all();
    if (this.isHybrid) {
      this.ChangeEvent.emit("Change");
    }
    return res.map((e: any) => {
      return this.mapOutRow(e);
    });
  };
  Count = (where?: WhereInput<T>) => {
    const clause = BuildCond(where != undefined ? where : {}, this.Schema);
    const row = this.InternalDBReference.query(
      `SELECT COUNT(*) as count FROM "${this.Name}"${clause}`
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
