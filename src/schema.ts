export type OptimaTable<
  TColumns extends Record<string, OptimaField<any, any, any>>
> = TColumns;

export function Table<
  TColumns extends Record<string, OptimaField<any, any, any>>
>(name: string, cols: TColumns): TColumns & { __tableName: string } {
  return Object.assign({}, cols, { __tableName: name });
}

export type isFieldInsertOptional<T> =
  T extends OptimaField<any, infer O, any>
    ? O extends { primaryKey: true }
      ? O extends { autoIncrement: true }
        ? true   // primary + autoincrement → optional
        : false  // primary without autoincrement → required
      : O extends { notNull: infer N }
        ? [N] extends [true]
          ? false // explicitly notNull: true → required
          : true
        : true
    : false;

export type FieldReference<T> = T extends OptimaField<any, any, infer R>
  ? R
  : null;

export type FieldReferenceMany<T> = T extends OptimaField<any, any, infer R>
  ? NonNullable<R> extends { Type: "Many" }
    ? true
    : false
  : false;

export type IsReferenceField<T> = FieldReference<T> extends { Table: any }
  ? true
  : false;

export type Equals<X, Y> = (<T>() => T extends /*1st*/ X
  ? 1
  : 2) /*2nd*/ extends <T>() => T extends /*3rd*/ Y ? 1 : 2
  ? true
  : false;

export type FieldReferencesTable<
  F extends OptimaField<any, any, any>,
  B extends OptimaTable<any>
> = IsReferenceField<F> extends true
  ? FieldReference<F> extends { Table: infer TB }
    ? Equals<TB, B>
    : false
  : false;

export type TableReferencesTableByMany<
  A extends OptimaTable<any>,
  B extends OptimaTable<any>
> = HasAtLeastOneTrue<{
  [K in keyof A]: A[K] extends OptimaField<any, any, any>
    ? FieldReferencesTable<A[K], B> extends true
      ? FieldReferenceMany<A[K]> extends true
        ? true
        : false
      : false
    : false;
}>;

export type HasAtLeastOneTrue<T extends Record<string, boolean>> =
  true extends T[keyof T] ? true : false;

export type TableReferencesTable<
  A extends OptimaTable<any>,
  B extends OptimaTable<any>
> = HasAtLeastOneTrue<{
  [K in keyof A]: A[K] extends OptimaField<any, any, any>
    ? FieldReferencesTable<A[K], B>
    : false;
}>;

export type GetNonNeverStringValues<T extends Record<string, any>> = {
  // Iterate over each key K in the input type T
  [K in keyof T]: T[K] extends never // Check if the value type T[K] is 'never'
    ? never // If it's 'never', replace it with 'never' (to be filtered out later)
    : T[K]; // Otherwise, keep the original value type T[K]
}[keyof T];

export type ExtendTables<
  A extends OptimaTable<any>,
  S extends Record<string, OptimaTable<any>>
> = GetNonNeverStringValues<{
  [K in keyof S]: TableReferencesTable<S[K], A> extends true ? K : never;
}>;

// ---------- Fix the class to actually use Ref ----------
export class OptimaField<
  TsType,
  Options extends {
    notNull?: boolean;
    default?: any;
    enum?: any[];
    primaryKey?: boolean;
    unique?: boolean;
    check?: string;
    autoIncrement?: boolean;
  },
  Ref extends {
    Table: OptimaTable<any> | string; // allow typed table or plain string
    Field: string;
    Type: "Many" | "One";
    TableName:string
  } | null = null
> {
  private Type: FieldTypes;
  private SQLType: string;
  private Default?: TsType | null;
  private Enum?: TsType[] | null;
  private Reference?: Ref | null; // <-- use Ref here
  private Unique?: boolean | null;
  private Check?: string | null;
  private NotNull?: boolean | null;
  private PrimaryKey?: boolean | null;
  private AutoIncrement?: boolean | null;

  constructor(type: FieldTypes, options: Options) {
    this.Type = type;
    this.NotNull = options?.notNull ? true : false;
    this.Default = options?.default ?? null;
    this.Enum = options?.enum ?? null;
    this.PrimaryKey = options?.primaryKey ?? null;
    this.Unique = options?.unique ?? null;
    this.Check = options?.check ?? null;
    this.AutoIncrement = options?.autoIncrement ?? null;

    const OptimaToSQLMAP: Record<FieldTypes, string> = {
      [FieldTypes.Email]: "TEXT",
      [FieldTypes.Text]: "TEXT",
      [FieldTypes.DateTime]: "TEXT",
      [FieldTypes.Day]: "TEXT",
      [FieldTypes.Password]: "TEXT",
      [FieldTypes.UUID]: "TEXT",
      [FieldTypes.Int]: "INTEGER",
      [FieldTypes.Boolean]: "INTEGER",
      [FieldTypes.Float]: "REAL",
      [FieldTypes.Json]: "TEXT",
      [FieldTypes.Array]: "TEXT",
    };
    this.SQLType = OptimaToSQLMAP[this.Type];
  }

  reference<
    TB extends OptimaTable<any>,
    F extends Exclude<keyof TB, "__tableName">,
    K extends "Many" | "One"
  >(
    Table: TB,
    Field: F,
    type: K
  ): OptimaField<TsType, Options, { Table: TB; Field: F & string; Type: K ,TableName:string}> {
    // store a typed object so Ref is preserved at the type level
    const ref = {
      Table,
      Field: Field as F & string,
      Type: type,
      TableName:Table["__tableName"]
    } as { Table: TB; Field: F & string; Type: K ,TableName:string};

    (this as any).Reference = ref;
    return this as any;
  }
}

// --------------------
// SQL Builders
// --------------------
// ---------- Make FieldToSQL robust to either typed table or string ----------
export const FieldToSQL = (field: OptimaField<any, any, any>): string => {
  const parts: string[] = [field["SQLType"]];
  if (field["PrimaryKey"]) parts.push("PRIMARY KEY");
  if (field["Type"] === FieldTypes.Int && field["AutoIncrement"]) {
    parts.push("AUTOINCREMENT");
  }
  if (field["NotNull"]) parts.push("NOT NULL");
  if (field["Unique"]) parts.push("UNIQUE");
  if (field["Default"] !== null && field["Default"] !== undefined) {
    const defVal =
      typeof field["Default"] === "string"
        ? `'${field["Default"]}'`
        : field["Default"];
    parts.push(`DEFAULT ${defVal}`);
  }
  if (field["Enum"]) {
    const enumVals = field["Enum"].map((v) => `'${v}'`).join(", ");
    parts.push(`CHECK (value IN (${enumVals}))`);
  }
  if (field["Check"]) parts.push(`CHECK (${field["Check"]})`);

  const ref = field["Reference"] as
    | {
        Table: OptimaTable<any> ;
        Field: string;
        Type: "Many" | "One";
        TableName:{ __tableName: string } | string
      }
    | null
    | undefined;

  if (ref) {
    const tableName = ref.TableName;
    parts.push(`REFERENCES ${tableName}(${ref.Field}) ON DELETE CASCADE`);
  }

  return parts.join(" ");
};

export const TableToSQL = (table: OptimaTable<any>, name: string): string => {
  const t: any = table as any;
  const cols: Record<string, OptimaField<any, any>> = t;
  delete cols.__tableName
  const colDefs = Object.entries(cols)
    .map(([colName, field]) => `\`${colName}\` ${FieldToSQL(field)}`);
  return `CREATE TABLE IF NOT EXISTS \`${name}\` (\n  ${colDefs.join(
    ",\n  "
  )}\n);`;
};

// --------------------
// Formatters
// --------------------
export function applyFormatIn(field: OptimaField<any, any>, value: any): any {
  if (value === undefined) return undefined;
  if (value === null) return null;

  switch (field["Type"]) {
    case FieldTypes.Boolean:
      return value ? 1 : 0;
    case FieldTypes.Int:
      return typeof value === "number" ? Math.trunc(value) : Number(value);
    case FieldTypes.Float:
      return typeof value === "number" ? value : Number(value);
    case FieldTypes.Json:
    case FieldTypes.Array:
      return typeof value === "string" ? value : JSON.stringify(value);
    case FieldTypes.DateTime:
      return value instanceof Date ? value.toISOString() : String(value);
    case FieldTypes.Day:
      if (value instanceof Date) return value.toISOString().slice(0, 10);
      if (typeof value === "string") return value.slice(0, 10);
      return String(value);
    default:
      return value;
  }
}

export function applyFormatOut(field: OptimaField<any, any>, value: any): any {
  if (value === undefined) return undefined;
  if (value === null) return null;

  switch (field["Type"]) {
    case FieldTypes.Boolean:
      return value === 1 || value === true;
    case FieldTypes.Int:
    case FieldTypes.Float:
      return typeof value === "number" ? value : Number(value);
    case FieldTypes.Json:
    case FieldTypes.Array:
      if (typeof value === "string") {
        try {
          return JSON.parse(value);
        } catch {
          return value;
        }
      }
      return value;
    case FieldTypes.DateTime:
      return typeof value === "string" ? new Date(value) : value;
    case FieldTypes.Day:
      return typeof value === "string" ? value : String(value);
    default:
      return value;
  }
}

// --------------------
// Config Types
// --------------------
export type FieldOptions = {
  notNull?: boolean;
  default?: any;
  enum?: any[];
  primaryKey?: boolean;
  unique?: boolean;
  check?: string;
  autoIncrement?: boolean;
};

export interface Reference {
  Table: string;
  Field: string;
  Type: "MANY" | "ONE";
}

export enum FieldTypes {
  Int = "INT",
  Float = "FLOAT",
  Boolean = "BOOLEAN",
  Text = "TEXT",
  Password = "PASSWORD",
  Email = "EMAIL",
  DateTime = "DATETIME",
  Day = "DATE",
  UUID = "UUID",
  Array = "ARRAY",
  Json = "JSON",
}

type BaseFieldOptions = {
  notNull?: boolean;
  default?: any;
  enum?: any[];
  primaryKey?: boolean;
  unique?: boolean;
  check?: string;
  autoIncrement?: boolean;
};
export function Int<O extends BaseFieldOptions>(options?: O) {
  return new OptimaField<number, { [K in keyof O]: O[K] }>(
    FieldTypes.Int,
    (options ?? {}) as { [K in keyof O]: O[K] }
  );
}

export function Float<O extends BaseFieldOptions>(options?: O) {
  return new OptimaField<number, { [K in keyof O]: O[K] }>(
    FieldTypes.Float,
    (options ?? {}) as { [K in keyof O]: O[K] }
  );
}

export function Boolean<O extends BaseFieldOptions>(options?: O) {
  return new OptimaField<boolean, { [K in keyof O]: O[K] }>(
    FieldTypes.Boolean,
    (options ?? {}) as { [K in keyof O]: O[K] }
  );
}

export function Text<O extends BaseFieldOptions>(options?: O) {
  return new OptimaField<string, { [K in keyof O]: O[K] }>(
    FieldTypes.Text,
    (options ?? {}) as { [K in keyof O]: O[K] }
  );
}

export function Password<O extends BaseFieldOptions>(options?: O) {
  return new OptimaField<string, { [K in keyof O]: O[K] }>(
    FieldTypes.Password,
    (options ?? {}) as { [K in keyof O]: O[K] }
  );
}

export function Email<O extends BaseFieldOptions>(options?: O) {
  return new OptimaField<string, { [K in keyof O]: O[K] }>(
    FieldTypes.Email,
    (options ?? {}) as { [K in keyof O]: O[K] }
  );
}

export function DateTime<O extends BaseFieldOptions>(options?: O) {
  return new OptimaField<Date, { [K in keyof O]: O[K] }>(
    FieldTypes.DateTime,
    (options ?? {}) as { [K in keyof O]: O[K] }
  );
}

export function Day<O extends BaseFieldOptions>(options?: O) {
  return new OptimaField<string, { [K in keyof O]: O[K] }>(
    FieldTypes.Day,
    (options ?? {}) as { [K in keyof O]: O[K] }
  );
}

export function UUID<O extends BaseFieldOptions>(options?: O) {
  return new OptimaField<string, { [K in keyof O]: O[K] }>(
    FieldTypes.UUID,
    (options ?? {}) as { [K in keyof O]: O[K] }
  );
}

export function Array<O extends BaseFieldOptions>(options?: O) {
  return new OptimaField<number[] | string[], { [K in keyof O]: O[K] }>(
    FieldTypes.Array,
    (options ?? {}) as { [K in keyof O]: O[K] }
  );
}

export function Json<O extends BaseFieldOptions>(options?: O) {
  return new OptimaField<Record<string, any>, { [K in keyof O]: O[K] }>(
    FieldTypes.Json,
    (options ?? {}) as { [K in keyof O]: O[K] }
  );
}


