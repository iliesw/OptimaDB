import { OptimaDB, TableToSQL } from "./../src";
import * as Schema from "./schema";
const DB = new OptimaDB(Schema);

console.log(TableToSQL(Schema.Users, "a"));

console.log(
  DB.Tables.Users.Insert(
    {
      Email: "hello@p.com",
      JSON: { f: 1, g: 2 },
    },
    true
  )
);
console.log(
  DB.Tables.Users.Get({
    JSON:{f:1,g:2},
  })
);
