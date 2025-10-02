import { Database } from "bun:sqlite";
import type { OptimaTable } from "./schema";
import * as fs from "node:fs";
import * as path from "node:path";
import { OptimaTB } from "./table";
import { resolve } from "path";

export type OptimaTablesFromSchema<
  S extends Record<string, OptimaTB<any, any>>
> = {
  [K in keyof S]: OptimaTB<S[K], any>;
};

export class OptimaDB<S extends Record<string, OptimaTable<any>>> {
  private Path: string = "";
  private InternalDB: Database;
  public Tables: {
    [K in keyof S]: OptimaTB<S[K], S, K & string>;
  };
  private Mode: "Disk" | "Memory" | "Hybrid" = "Memory";
  private SchemaRef: S;

  private LoadExt = () => {
    const Extentions = ["array.dll","time.dll","uuid.dll","vec.dll"]
    Extentions.forEach(dll=>{
      const extensionPath = resolve(__dirname, "./../Extentions/"+dll);
      this.InternalDB.loadExtension(extensionPath);
    })
  };

  constructor(
    Schema: S,
    options?: {
      mode: "Disk" | "Memory" | "Hybrid";
      path: string;
    }
  ) {
    this.SchemaRef = Schema;
    options = options ?? {
      mode: "Memory",
      path: "",
    };

    switch (options?.mode) {
      case "Disk": {
        this.Mode = "Disk";
        this.Path = options.path;
        const dbFile = this.ensureDbPath();
        this.InternalDB = new Database(dbFile, { create: true });
        break;
      }
      case "Memory": {
        this.Mode = "Memory";
        this.InternalDB = new Database(":memory:");
        break;
      }
      // case "Hybrid": {
      //   this.Mode = "Hybrid";
      //   this.Path = options.path;
      //   this.LoadFromDisk();
      //   this.InternalDB.exec("PRAGMA journal_mode = WAL;");
      //   this.InternalDB.exec("PRAGMA synchronous = NORMAL;"); // balance speed + durability
      //   this.InternalDB.exec("PRAGMA temp_store = MEMORY;"); // faster temp tables
      //   this.InternalDB.exec("PRAGMA mmap_size = 30000000000;"); // optional: use mmap for reads
      //   this.InternalDB.exec("PRAGMA cache_size = 100000;");
      //   this.InternalDB.exec("PRAGMA locking_mode = EXCLUSIVE;");
      //   this.InternalDB.exec("PRAGMA automatic_index = ON;");

      //   const saveHandler = () => {
      //     try {
      //       this.SaveToDisk();
      //     } catch (e) {
      //       console.error("Error saving database on signal:", e);
      //     }
      //   };

      //   const signals = [
      //     "exit",
      //     "SIGINT",
      //     "SIGTERM",
      //     "SIGQUIT",
      //     "SIGHUP",
      //     "uncaughtException",
      //     "unhandledRejection",
      //   ];

      //   for (const sig of signals) {
      //     if (sig === "exit") {
      //       process.once(sig, () => {
      //         saveHandler();
      //       });
      //     } else if (
      //       sig === "uncaughtException" ||
      //       sig === "unhandledRejection"
      //     ) {
      //       process.once(sig, (err) => {
      //         saveHandler();
      //         console.error(`Process ${sig}:`, err);
      //         process.exit(1);
      //       });
      //     } else {
      //       process.once(sig, () => {
      //         saveHandler();
      //         process.exit(0);
      //       });
      //     }
      //   }
      //   break;
      // }
    }
    this.InternalDB.exec("PRAGMA journal_mode = WAL;");
    this.InternalDB.exec("PRAGMA synchronous = NORMAL;"); // balance speed + durability
    this.InternalDB.exec("PRAGMA temp_store = MEMORY;"); // faster temp tables
    this.InternalDB.exec("PRAGMA mmap_size = 30000000000;"); // optional: use mmap for reads
    this.InternalDB.exec("PRAGMA cache_size = 100000;");
    this.InternalDB.exec("PRAGMA automatic_index = ON;");

    this.LoadExt();

    this.Tables = {} as { [K in keyof S]: OptimaTB<S[K], S, K & string> };
    for (const tableName in Schema) {
      const key = tableName as keyof S;
      const tableDef = Schema[key];
      this.Tables[key] = new OptimaTB(
        this.InternalDB,
        tableName as keyof S & string,
        tableDef,
        Schema,
        this,
        options.mode == "Hybrid"
      ) as any;
    }

    // Tables are automatically initialized with schema creation in InitTable method
  }

  Batch = (fn: Function) => {
    this.InternalDB.run("BEGIN");
    try {
      fn();
      this.InternalDB.run("COMMIT");
    } catch (e) {
      this.InternalDB.run("ROLLBACK");
      throw e;
    }
  };

  Raw = (q:string) => {
    return this.InternalDB.query(q).all()
  }

  private LoadFromDisk(): void {
    const dbPath = path.join(this.Path, "db.sqlite");
    const exist = fs.existsSync(dbPath);

    if (!exist) {
      const inMemoryDb = new Database(":memory:");
      this.ensureDbPath();
      this.InternalDB = inMemoryDb;
      return;
    }

    const inMemoryDb = new Database(":memory:");
    try {
      const escapedDiskDbPath = dbPath.replace(/\\/g, "\\\\");
      inMemoryDb.exec(`ATTACH DATABASE '${escapedDiskDbPath}' AS source_db;`);

      const tables = inMemoryDb
        .query(
          `SELECT name FROM source_db.sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';`
        )
        .all() as { name: string }[];

      inMemoryDb.transaction(() => {
        for (const table of tables) {
          const tableName = table.name;
          const createStmtResult = inMemoryDb
            .query(
              `SELECT sql FROM source_db.sqlite_master WHERE type='table' AND name='${tableName}';`
            )
            .get() as { sql: string } | undefined;

          if (createStmtResult && createStmtResult.sql) {
            inMemoryDb.exec(createStmtResult.sql);
            inMemoryDb.exec(
              `INSERT INTO ${tableName} SELECT * FROM source_db.${tableName};`
            );
          } else {
            console.warn(
              `Could not find CREATE TABLE statement for '${tableName}'. Skipping.`
            );
          }
        }
      })();
    } catch (error: any) {
      console.error("Error during database copy via ATTACH:", error.message);
      inMemoryDb.close();
      throw error;
    } finally {
      inMemoryDb.exec("DETACH DATABASE source_db;");
    }
    this.InternalDB = inMemoryDb;
  }

  private SaveToDisk() {
    const content = this.InternalDB.serialize();
    fs.writeFileSync(path.join(this.Path, "db.sqlite"), content);
  }

  private ensureDbPath(): string {
    const base = this.Path ? this.Path : ".";
    try {
      fs.mkdirSync(base, { recursive: true });
    } catch {}
    return path.join(base, "db.sqlite");
  }
}
