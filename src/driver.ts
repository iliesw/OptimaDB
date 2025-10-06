// Database.ts
let Database: any;
if (typeof window !== "undefined" && typeof window.document !== "undefined") {
  Database = "Not Supported Yet";
} else {
  // --- Server-side ---
  try {
    const { Database: BunDatabase } = require("bun:sqlite");
    Database = BunDatabase;
  } catch {
    const { Database: NodeDatabase } = require("better-sqlite3");
    Database = NodeDatabase;
  }
}

export { Database };
