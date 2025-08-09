export const TABLE_META = Symbol("OPTIMA_TABLE_META");

export type OptimaTableMeta = {
  Name: string;
  toSQL: () => string;
};

export type OptimaTableDef<TColumns extends Record<string, OptimaField>> =
  TColumns;

export function Table<TColumns extends Record<string, OptimaField>>(
  name: string,
  cols: TColumns
): OptimaTableDef<TColumns> {
  class OptimaTableInstance {
    #name: string;
    #cols: TColumns;

    constructor(tableName: string, columns: TColumns) {
      this.#name = tableName;
      this.#cols = columns;

      // Attach column fields directly to the instance for autocomplete
      Object.assign(this, columns);

      // Attach non-enumerable metadata for internal use
      Object.defineProperty(this, TABLE_META, {
        value: {
          Name: this.#name,
          toSQL: () => this.#toSQL(),
        } as OptimaTableMeta,
        enumerable: false,
        configurable: false,
        writable: false,
      });
    }

    // Private SQL builder function
    #toSQL(): string {
      const colDefs = Object.entries(this.#cols).map(([colName, field]) => {
        return `"${colName}" ${field["toSQL"]()}`;
      });
      return `CREATE TABLE IF NOT EXISTS "${this.#name}" (\n  ${colDefs.join(",\n  ")}\n);`;
    }
  }

  // Return instance typed as columns + hidden meta
  return new OptimaTableInstance(name, cols) as unknown as OptimaTableDef<TColumns>;
}
export class OptimaField<
  T = any,
  TNotNull extends boolean = false,
  THasDefault extends boolean = false
> {
  private Type: FieldTypes;
  private SQLType: string;
  private NotNull?: boolean | null;
  private Default?: T | null;
  private Enum?: T[] | null;
  private Reference?: Reference | null;
  private PrimaryKey?: boolean | null;
  private Unique?: boolean | null;
  private Check?: string | null;
  // Runtime value transformers
  private FormatInFn?: (value: any) => any;
  private FormatOutFn?: (value: any) => any;

  constructor(
    type: FieldTypes,
    options?: FieldOptions & {
      default?: T;
      enum?: T[];
      primaryKey?: boolean;
      unique?: boolean;
      check?: string;
    }
  ) {
    this.Type = type;
    this.NotNull = options?.notNull ?? null;
    this.Default = options?.default ?? null;
    this.Enum = options?.enum ?? null;
    this.PrimaryKey = options?.primaryKey ?? null;
    this.Unique = options?.unique ?? null;
    this.Check = options?.check ?? null;

    const OptimaToSQLMAP = {
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

  private toSQL = () => {
    const parts: string[] = [this.SQLType];
    if (this.PrimaryKey) parts.push("PRIMARY KEY");
    if (this.NotNull) parts.push("NOT NULL");
    if (this.Unique) parts.push("UNIQUE");
    if (this.Default !== null && this.Default !== undefined) {
      const defVal =
        typeof this.Default === "string" ? `'${this.Default}'` : this.Default;
      parts.push(`DEFAULT ${defVal}`);
    }
    if (this.Enum) {
      const enumVals = this.Enum.map((v) => `'${v}'`).join(", ");
      parts.push(`CHECK (value IN (${enumVals}))`);
    }
    if (this.Check) {
      parts.push(`CHECK (${this.Check})`);
    }
    if (this.Reference) {
      parts.push(
        `REFERENCES ${this.Reference.Table}(${this.Reference.Field}) ON DELETE CASCADE`
      );
    }
    return parts.join(" ");
  };

  // Introspection helpers for runtime validation / DX
  isNotNullField = () => {
    return this.NotNull === true;
  };
  hasDefaultValue = () => {
    return this.Default !== null && this.Default !== undefined;
  };
  getDefaultValue = () => this.Default;

  reference = (ref: () => [any] | any) => {
    const refStr = ref.toString().replace(/\s/g, "");
    let match = refStr.match(/\(\)=>\[(\w+)\.(\w+)\]/);
    let isArray = false;
    let table: string | undefined;
    let field: string | undefined;

    if (match) {
      isArray = true;
      table = match[1];
      field = match[2];
    } else {
      match = refStr.match(/\(\)=>(\w+)\.(\w+)/);
      if (match) {
        isArray = false;
        table = match[1];
        field = match[2];
      }
    }

    if (!table || !field) {
      throw new Error(
        "Invalid reference function format. Expected () => [Table.Field] or () => Table.Field"
      );
    }

    this.Reference = {
      Field: field,
      Table: table,
      Type: isArray ? "MANY" : "ONE",
    };
    return this;
  };

  // Chainable setters to customize value formatting
  formatIn = (fn: (value: any) => any) => {
    this.FormatInFn = fn;
    return this;
  };

  formatOut = (fn: (value: any) => any) => {
    this.FormatOutFn = fn;
    return this;
  };

  // Internal application of default/custom formatting for DB I/O
  applyFormatIn = (value: any) => {
    if (value === undefined) return undefined;
    if (value === null) return null;
    if (this.FormatInFn) return this.FormatInFn(value);
    switch (this.Type) {
      case FieldTypes.Boolean:
        return value ? 1 : 0;
      case FieldTypes.Int:
        return typeof value === "number" ? Math.trunc(value) : Number(value);
      case FieldTypes.Float:
        return typeof value === "number" ? value : Number(value);
      case FieldTypes.Json:
      case FieldTypes.Array:
        return typeof value === "string" ? value : JSON.stringify(value);
      case FieldTypes.DateTime: {
        if (value instanceof Date) return value.toISOString();
        return String(value);
      }
      case FieldTypes.Day: {
          if (value instanceof Date) return value.toISOString().slice(0, 10);
          if (typeof value === "string") return value.slice(0, 10);
          return String(value);
      }
      default:
        return value;
    }
  };

  applyFormatOut = (value: any) => {
    if (value === undefined) return undefined;
    if (value === null) return null;
    if (this.FormatOutFn) return this.FormatOutFn(value);
    switch (this.Type) {
      case FieldTypes.Boolean:
        return value === 1 || value === true;
      case FieldTypes.Int:
      case FieldTypes.Float:
        return typeof value === "number" ? value : Number(value);
      case FieldTypes.Json:
      case FieldTypes.Array: {
        if (typeof value === "string") {
          try {
            return JSON.parse(value);
          } catch {
            return value;
          }
        }
        return value;
      }
      case FieldTypes.DateTime:
        return typeof value === "string" ? new Date(value) : value;
      case FieldTypes.Day:
        return typeof value === "string" ? value : String(value);
      default:
        return value;
    }
  };

}

export type FieldOptions = {
  notNull?: boolean;
  default?: any;
  enum?: any[];
  primaryKey?: boolean;
  unique?: boolean;
  check?: string;
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

// Field helpers
// Typed factory overloads to carry notNull/default flags into OptimaField generics
export function Int(options: FieldOptions & { notNull: true; default: any }): OptimaField<number, true, true>;
export function Int(options: FieldOptions & { notNull: true }): OptimaField<number, true, false>;
export function Int(options: FieldOptions & { default: any }): OptimaField<number, false, true>;
export function Int(options?: FieldOptions): OptimaField<number, false, false>;
export function Int(options?: FieldOptions) {
  return new OptimaField<number>(FieldTypes.Int, options);
}

export function Float(options: FieldOptions & { notNull: true; default: any }): OptimaField<number, true, true>;
export function Float(options: FieldOptions & { notNull: true }): OptimaField<number, true, false>;
export function Float(options: FieldOptions & { default: any }): OptimaField<number, false, true>;
export function Float(options?: FieldOptions): OptimaField<number, false, false>;
export function Float(options?: FieldOptions) {
  return new OptimaField<number>(FieldTypes.Float, options);
}

export function Boolean(options: { notNull: true; default: any }): OptimaField<boolean, true, true>;
export function Boolean(options: { notNull: true }): OptimaField<boolean, true, false>;
export function Boolean(options: { default: any }): OptimaField<boolean, false, true>;
export function Boolean(options?: { notNull?: boolean; default?: any }): OptimaField<boolean, false, false>;
export function Boolean(options?: { notNull?: boolean; default?: any }) {
  return new OptimaField<boolean>(FieldTypes.Boolean, options);
}

export function Text(options: FieldOptions & { notNull: true; default: any }): OptimaField<string, true, true>;
export function Text(options: FieldOptions & { notNull: true }): OptimaField<string, true, false>;
export function Text(options: FieldOptions & { default: any }): OptimaField<string, false, true>;
export function Text(options?: FieldOptions): OptimaField<string, false, false>;
export function Text(options?: FieldOptions) {
  return new OptimaField<string>(FieldTypes.Text, options);
}

export function Password(options: { notNull: true }): OptimaField<string, true, false>;
export function Password(options?: { notNull?: boolean }): OptimaField<string, false, false>;
export function Password(options?: { notNull?: boolean }) {
  return new OptimaField<string>(FieldTypes.Password, options);
}

export function Email(options: FieldOptions & { notNull: true; default: any }): OptimaField<string, true, true>;
export function Email(options: FieldOptions & { notNull: true }): OptimaField<string, true, false>;
export function Email(options: FieldOptions & { default: any }): OptimaField<string, false, true>;
export function Email(options?: FieldOptions): OptimaField<string, false, false>;
export function Email(options?: FieldOptions) {
  return new OptimaField<string>(FieldTypes.Email, options);
}

export function Day(options: FieldOptions & { notNull: true; default: any }): OptimaField<string, true, true>;
export function Day(options: FieldOptions & { notNull: true }): OptimaField<string, true, false>;
export function Day(options: FieldOptions & { default: any }): OptimaField<string, false, true>;
export function Day(options?: FieldOptions): OptimaField<string, false, false>;
export function Day(options?: FieldOptions) {
  return new OptimaField<string>(FieldTypes.Day, options);
}

export function DateTime(options: FieldOptions & { notNull: true; default: any }): OptimaField<Date, true, true>;
export function DateTime(options: FieldOptions & { notNull: true }): OptimaField<Date, true, false>;
export function DateTime(options: FieldOptions & { default: any }): OptimaField<Date, false, true>;
export function DateTime(options?: FieldOptions): OptimaField<Date, false, false>;
export function DateTime(options?: FieldOptions) {
  return new OptimaField<Date>(FieldTypes.DateTime, options);
}

export function UUID(options: FieldOptions & { notNull: true; default: any }): OptimaField<string, true, true>;
export function UUID(options: FieldOptions & { notNull: true }): OptimaField<string, true, false>;
export function UUID(options: FieldOptions & { default: any }): OptimaField<string, false, true>;
export function UUID(options?: FieldOptions): OptimaField<string, false, false>;
export function UUID(options?: FieldOptions) {
  return new OptimaField<string>(FieldTypes.UUID, options);
}

export function Array(options: { notNull: true; default: any }): OptimaField<any[], true, true>;
export function Array(options: { notNull: true }): OptimaField<any[], true, false>;
export function Array(options: { default: any }): OptimaField<any[], false, true>;
export function Array(options?: { notNull?: boolean; default?: any }): OptimaField<any[], false, false>;
export function Array(options?: { notNull?: boolean; default?: any }) {
  return new OptimaField<any[]>(FieldTypes.Array, options);
}

export function Json(options: { notNull: true; default: any }): OptimaField<any, true, true>;
export function Json(options: { notNull: true }): OptimaField<any, true, false>;
export function Json(options: { default: any }): OptimaField<any, false, true>;
export function Json(options?: { notNull?: boolean; default?: any }): OptimaField<any, false, false>;
export function Json(options?: { notNull?: boolean; default?: any }) {
  return new OptimaField<any>(FieldTypes.Json, options);
}
