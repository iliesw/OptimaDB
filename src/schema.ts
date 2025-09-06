export type OptimaTable<
  TColumns extends Record<string, OptimaField<any, any, any>>
> = TColumns;

export type GetType<
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

export type OptimaFieldToTS<F extends OptimaField<any, any, any>> =
  F extends OptimaField<infer K, any, any> ? K : never;

export type ColumnWhere<T> = T | null | T[] | WhereOperatorObject<T>;
export type BasicWhere<TDef extends OptimaTable<Record<string, any>>> = {
  [K in keyof TDef as K extends "__tableName" ? never : K]?: ColumnWhere<
    OptimaFieldToTS<TDef[K]>
  >;
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
  $includes?: T extends (infer U)[] ? U : T;
};
export type UpdateChanges<T extends OptimaTable<Record<string, any>>> =
  Partial<{
    [K in keyof T as K extends "__tableName" ? never : K]: OptimaFieldToTS<
      T[K]
    >;
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
  } & {
    // Optional keys
    [K in keyof TDef as K extends "__tableName"
      ? never
      : isFieldInsertOptional<TDef[K]> extends true
      ? K
      : never]?: OptimaFieldToTS<TDef[K]>;
  };

export type isFieldInsertOptional<T> = T extends OptimaField<any, infer O, any>
  ? O extends { primaryKey: true }
    ? O extends { autoIncrement: true }
      ? true // primary + autoincrement → optional
      : false // primary without autoincrement → required
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
    check?: (value: TsType) => boolean;
    autoIncrement?: boolean;
  },
  Ref extends {
    Table: OptimaTable<any> | string; // allow typed table or plain string
    Field: string;
    Type: "Many" | "One";
    TableName: string;
  } | null = null
> {
  private Type: FieldTypes;
  private SQLType: string;
  private Default?: TsType | null;
  private Enum?: TsType[] | null;
  private Reference?: Ref | null; // <-- use Ref here
  private Unique?: boolean | null;
  private Check?: ((value: TsType) => boolean) | null;
  private NotNull?: boolean | null;
  private PrimaryKey?: boolean | null;
  private AutoIncrement?: boolean | null;

  constructor(type: FieldTypes, options: Options) {
    this.Type = type;
    this.NotNull = options?.notNull ? true : false;
    this.Default = options?.default ?? null;
    this.Enum = options?.enum ?? null;
    this.PrimaryKey = options?.primaryKey ?? options?.autoIncrement ?? null;
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
  ): OptimaField<
    TsType,
    Options,
    { Table: TB; Field: F & string; Type: K; TableName: string }
  > {
    // store a typed object so Ref is preserved at the type level
    const ref = {
      Table,
      Field: Field as F & string,
      Type: type,
      TableName: Table["__tableName"],
    } as { Table: TB; Field: F & string; Type: K; TableName: string };

    (this as any).Reference = ref;
    return this as any;
  }
}

export function Table<
  TColumns extends Record<string, OptimaField<any, any, any>>
>(name: string, cols: TColumns): TColumns & { __tableName: string } {
  return Object.assign({}, cols, { __tableName: name });
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
    let defVal: string;

    // Handle JSON and Array types specially for default values
    if (
      field["Type"] === FieldTypes.Json ||
      field["Type"] === FieldTypes.Array
    ) {
      // For JSON/Array fields, serialize the default value as JSON
      defVal = `'${JSON.stringify(field["Default"]).replace(/'/g, "''")}'`;
    } else if (
      field["Type"] === FieldTypes.DateTime &&
      field["Default"] instanceof Date
    ) {
      // For DateTime fields, format as ISO string
      defVal = `'${field["Default"].toISOString().replace(/'/g, "''")}'`;
    } else if (typeof field["Default"] === "string") {
      // For string fields, escape single quotes
      defVal = `'${field["Default"].replace(/'/g, "''")}'`;
    } else {
      // For other types, convert to string
      defVal = String(field["Default"]);
    }

    parts.push(`DEFAULT ${defVal}`);
  }

  const ref = field["Reference"] as
    | {
        Table: OptimaTable<any> | string;
        Field: string;
        Type: "Many" | "One";
        TableName: string;
      }
    | null
    | undefined;

  if (ref) {
    const tableName = ref.TableName;
    parts.push(`REFERENCES "${tableName}"("${ref.Field}") ON DELETE CASCADE`);
  }

  return parts.join(" ");
};

export const TableToSQL = (table: OptimaTable<any>, name: string): string => {
  const t: any = table as any;
  const cols: Record<string, OptimaField<any, any>> = t;
  delete cols.__tableName;
  const colDefs = Object.entries(cols).map(
    ([colName, field]) => `\`${colName}\` ${FieldToSQL(field)}`
  );
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
export type FieldOptions<T = any> = {
  notNull?: boolean;
  default?: any;
  enum?: any[];
  primaryKey?: boolean;
  unique?: boolean;
  check?: (value: T) => boolean;
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

type BaseFieldOptions<T = any> = {
  notNull?: boolean;
  default?: any;
  enum?: any[];
  primaryKey?: boolean;
  unique?: boolean;
  check?: (value: T) => boolean;
  autoIncrement?: boolean;
};
export function Int<O extends BaseFieldOptions<number>>(options?: O) {
  return new OptimaField<number, { [K in keyof O]: O[K] }>(
    FieldTypes.Int,
    (options ?? {}) as { [K in keyof O]: O[K] }
  );
}

export function Float<O extends BaseFieldOptions<number>>(options?: {
  notNull?: boolean;
  default?: any;
  enum?: any[];
  unique?: boolean;
  check?: (value: number) => boolean;
}) {
  const FloatOptions: BaseFieldOptions<number> = {
    autoIncrement: false,
    primaryKey: false,
    unique: options?.unique,
    enum: options?.enum,
    check: options?.check,
    default: options?.default,
    notNull: options?.notNull,
  };
  return new OptimaField<number, { [K in keyof O]: O[K] }>(
    FieldTypes.Float,
    (FloatOptions ?? {}) as { [K in keyof O]: O[K] }
  );
}

export function Boolean<O extends BaseFieldOptions<boolean>>(options?: {
  check?: (value: boolean) => boolean;
  default?: boolean;
  notNull?: boolean;
}) {
  const BooleanOptions: BaseFieldOptions<boolean> = {
    autoIncrement: false,
    primaryKey: false,
    unique: false,
    enum: [false, true],
    check: options?.check,
    default: options?.default,
    notNull: options?.notNull,
  };
  return new OptimaField<boolean, { [K in keyof O]: O[K] }>(
    FieldTypes.Boolean,
    (BooleanOptions ?? {}) as { [K in keyof O]: O[K] }
  );
}

export function Text<O extends BaseFieldOptions<string>>(options?: {
  notNull?: boolean;
  default?: any;
  enum?: any[];
  primaryKey?: boolean;
  unique?: boolean;
  check?: (value: string) => boolean;
}) {
  const TextOptions: BaseFieldOptions<string> = {
    autoIncrement: false,
    primaryKey: options?.primaryKey,
    unique: options?.unique,
    enum: options?.enum,
    check: options?.check,
    default: options?.default,
    notNull: options?.notNull,
  };
  return new OptimaField<string, { [K in keyof O]: O[K] }>(
    FieldTypes.Text,
    (TextOptions ?? {}) as { [K in keyof O]: O[K] }
  );
}

export function Password<O extends BaseFieldOptions<string>>(options?: {
  notNull?: boolean;
  check?: (value: string) => boolean;
}) {
  const PasswordOptions: BaseFieldOptions<string> = {
    autoIncrement: false,
    primaryKey: false,
    unique: false,
    enum: undefined,
    check: options?.check,
    default: undefined,
    notNull: options?.notNull,
  };
  return new OptimaField<string, { [K in keyof O]: O[K] }>(
    FieldTypes.Password,
    (PasswordOptions ?? {}) as { [K in keyof O]: O[K] }
  );
}

export function Email<O extends BaseFieldOptions<string>>(options?: {
  notNull?: boolean;
  default?: any;
  enum?: any[];
  primaryKey?: boolean;
  unique?: boolean;
  check?: (value: string) => boolean;
}) {
  const EmailOptions: BaseFieldOptions<string> = {
    autoIncrement: false,
    primaryKey: options?.primaryKey,
    unique: options?.unique,
    enum: options?.enum,
    check: options?.check,
    default: options?.default,
    notNull: options?.notNull,
  };
  return new OptimaField<string, { [K in keyof O]: O[K] }>(
    FieldTypes.Email,
    (EmailOptions ?? {}) as { [K in keyof O]: O[K] }
  );
}

export function Time<O extends BaseFieldOptions<Date>>(options?: {
  notNull?: boolean;
  default?: any;
  enum?: any[];
  unique?: boolean;
  check?: (value: Date) => boolean;
}) {
  const DateTimeOptions: BaseFieldOptions<Date> = {
    autoIncrement: false,
    primaryKey: false,
    unique: options?.unique,
    enum: options?.enum,
    check: options?.check,
    default: options?.default,
    notNull: options?.notNull,
  };
  return new OptimaField<Date, { [K in keyof O]: O[K] }>(
    FieldTypes.DateTime,
    (DateTimeOptions ?? {}) as { [K in keyof O]: O[K] }
  );
}



export function UUID<O extends BaseFieldOptions<string>>(options?: {
  notNull?: boolean;
  enum?: any[];
  primaryKey?: boolean;
  check?: (value: string) => boolean;
}) {
  const UUIDOptions: BaseFieldOptions<string> = {
    autoIncrement: false,
    primaryKey: options?.primaryKey,
    unique: true,
    enum: options?.enum,
    check: options?.check,
    default: undefined,
    notNull: options?.notNull,
  };
  return new OptimaField<string, { [K in keyof O]: O[K] }>(
    FieldTypes.UUID,
    (UUIDOptions ?? {}) as { [K in keyof O]: O[K] }
  );
}

export function Array<O extends BaseFieldOptions<number[] | string[]>>(
  options?: {
    notNull?: boolean;
    default?: any;
    enum?: any[];
    unique?: boolean;
    check?: (value: number[] | string[]) => boolean;
  }
) {
  const ArrayOptions: BaseFieldOptions<number[] | string[]> = {
    autoIncrement: false,
    primaryKey: false,
    unique: options?.unique,
    enum: options?.enum,
    check: options?.check,
    default: options?.default,
    notNull: options?.notNull,
  };
  return new OptimaField<number[] | string[], { [K in keyof O]: O[K] }>(
    FieldTypes.Array,
    (ArrayOptions ?? {}) as { [K in keyof O]: O[K] }
  );
}

export function Json<O extends BaseFieldOptions<Record<string, any>>>(
  options?: {
    notNull?: boolean;
    default?: any;
    enum?: any[];
    unique?: boolean;
    check?: (value: number[] | string[]) => boolean;
  }
) {
  const JsonOptions: BaseFieldOptions<Record<string, any>> = {
    autoIncrement: false,
    primaryKey: false,
    unique: options?.unique,
    enum: options?.enum,
    check: options?.check,
    default: options?.default,
    notNull: options?.notNull,
  };
  return new OptimaField<Record<string, any>, { [K in keyof O]: O[K] }>(
    FieldTypes.Json,
    (JsonOptions ?? {}) as { [K in keyof O]: O[K] }
  );
}
