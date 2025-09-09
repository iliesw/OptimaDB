// db.ts
let sqlite: any;

// if (typeof Bun !== "undefined") {
//   // Running on Bun
// } else {
//   // Running on Node.js (use better-sqlite3)
// }
// sqlite = require("better-sqlite3");
sqlite = require("bun:sqlite");

export const Database = sqlite.Database;
