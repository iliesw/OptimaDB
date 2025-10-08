import { OptimaDB, TableToSQL } from "./../src";
import * as Schema from "./schema";
console.log("\n\n\nInsert : ");
const DB = new OptimaDB(Schema);
const M = 10000;

// Insert benchmark (Optima)
const start = Date.now();
DB.Batch(() => {
  for (let i = 0; i < M; i++) {
    DB.Tables.Posts.Insert({
      ID: i,
      UserID: i,
      Content: new Date(),
    });
  }
});
const end = Date.now();

const throughput = M / ((end - start) / 1000);
console.log(`Optima - Throughput: ${throughput.toFixed(2)} inserts/sec`);

// Insert benchmark (Sqlite Native)
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

// Select benchmark (Optima)
console.log("Get:");
const startGet = Date.now();
DB.Tables.Posts.Get();
const endGet = Date.now();

const throughputGet = M / ((endGet - startGet) / 1000);
console.log(`Optima - Throughput: ${throughputGet.toFixed(2)} select/sec`);

// Select benchmark (Sqlite Native)
const startGetNative = Date.now();
DBNative.query("Select * from Posts")
  .all()
  .map((e: any) => {
    return (e.Content = JSON.parse(e.Content));
  });
const endGetNative = Date.now();

const throughputGetNative = M / ((endGetNative - startGetNative) / 1000);
console.log(
  `Sqlite - Throughput: ${throughputGetNative.toFixed(2)} select/sec`
);

// Update benchmark (Optima)
console.log("Update:");
const startUpdate = Date.now();
DB.Batch(() => {
  for (let i = 0; i < M; i++) {
    DB.Tables.Posts.Update(
      { Content: new Date(Date.now() + 1000) },
      { ID: i },
    );
  }
});
const endUpdate = Date.now();

const throughputUpdate = M / ((endUpdate - startUpdate) / 1000);
console.log(`Optima - Throughput: ${throughputUpdate.toFixed(2)} updates/sec`);

// Update benchmark (Sqlite Native)
const startUpdateNative = Date.now();
DBNative.transaction(() => {
  for (let i = 0; i < M; i++) {
    DBNative.query("UPDATE Posts SET Content = ? WHERE ID = ?").all(
      JSON.stringify({
        title: `Updated Post ${i}`,
        content: `Updated Content ${i}`,
      }),
      i
    );
  }
})();
const endUpdateNative = Date.now();

const throughputUpdateNative =
  M / ((endUpdateNative - startUpdateNative) / 1000);
console.log(
  `Sqlite - Throughput: ${throughputUpdateNative.toFixed(2)} updates/sec`
);

// Delete benchmark (Optima)
console.log("Delete:");
const startDelete = Date.now();
DB.Batch(() => {
  for (let i = 0; i < M; i++) {
    DB.Tables.Posts.Delete({ ID: i });
  }
});
const endDelete = Date.now();

const throughputDelete = M / ((endDelete - startDelete) / 1000);
console.log(`Optima - Throughput: ${throughputDelete.toFixed(2)} deletes/sec`);

// Delete benchmark (Sqlite Native)
const startDeleteNative = Date.now();
DBNative.transaction(() => {
  for (let i = 0; i < M; i++) {
    DBNative.query("DELETE FROM Posts WHERE ID = ?").all(i);
  }
})();
const endDeleteNative = Date.now();

const throughputDeleteNative =
  M / ((endDeleteNative - startDeleteNative) / 1000);
console.log(
  `Sqlite - Throughput: ${throughputDeleteNative.toFixed(2)} deletes/sec\n\n\n`
);
