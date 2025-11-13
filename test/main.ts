import { OptimaDB, TableToSQL } from "./../src";
import * as Schema from "./schema";
const DB = new OptimaDB(Schema, {
  mode: "Disk",
  path: "a",
});

const e = DB.Tables.Users.Insert({
  Salary:50
},true);
console.log(e)
console.log(
  DB.Tables.Users.Get({
    Dates: e.Dates.toISOString(),
  })
);
