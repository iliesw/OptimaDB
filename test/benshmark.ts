import { OptimaDB, TableToSQL } from "@inflector/db";
import * as Schema from "./schema";
console.log("\n\n\nInsert : ");
const DB = new OptimaDB(Schema);
const M = 100000;

const start = Date.now();
DB.Batch(() => {
  for (let i = 0; i < M; i++) {
    DB.Tables.Posts.Insert({
      ID: i,
      UserID: i,
      Content: {
        title: `Post ${i}`,
        content: `Content ${i}`,
      },
    });
  }
});
const end = Date.now();

const throughput = M / ((end - start) / 1000);
console.log(`Optima - Throughput: ${throughput.toFixed(2)} inserts/sec`);

import Database from "bun:sqlite";

const DBNative = new Database(":memory:");
DBNative.exec("PRAGMA journal_mode = WAL;");
DBNative.exec("PRAGMA synchronous = NORMAL;"); // balance speed + durability
DBNative.exec("PRAGMA temp_store = MEMORY;"); // faster temp tables
DBNative.exec("PRAGMA mmap_size = 30000000000;"); // optional: use mmap for reads
const SQLCreateTable = TableToSQL(Schema.Posts, "Posts");
DBNative.query(SQLCreateTable).run();
const startNative = Date.now();
DBNative.transaction(() => {
  for (let i = 0; i < M; i++) {
    DBNative.query("INSERT INTO Posts values(?,?,?)").all(
      i,
      i,
      JSON.stringify({
        title: `Post ${i}`,
        content: `Content ${i}`,
      })
    );
  }
})();
const endNative = Date.now();

const throughputNative = M / ((endNative - startNative) / 1000);
console.log(`Sqlite - Throughput: ${throughputNative.toFixed(2)} inserts/sec`);
console.log("Get:");
const startGet = Date.now();
DB.Tables.Posts.Get();
const endGet = Date.now();

const throughputGet = M / ((endGet - startGet) / 1000);
console.log(`Optima - Throughput: ${throughputGet.toFixed(2)} select/sec`);
const startGetNative = Date.now();
DBNative.query("Select * from Posts")
  .all()
  .map((e) => {
    return e.Content = JSON.parse(e.Content);
  });
const endGetNative = Date.now();

const throughputGetNative = M / ((endGetNative - startGetNative) / 1000);
console.log(
  `Sqlite - Throughput: ${throughputGetNative.toFixed(2)} select/sec\n\n\n`
);
