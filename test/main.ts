import { OptimaDB, TableToSQL, Int, Email } from "../src";
import * as Schema from "./schema";

const DB = new OptimaDB(Schema);

DB.Tables.Users.Insert({
Email:"",
Password:""
});

// This should cause a TypeScript error because Email is required (notNull: true)
// DB.Tables.Users.Insert({
//   // Missing Email - should cause error
// });

console.log(
  DB.Tables.Users.Get(
    { Array: { $includes: 123 } },
    {
      Extend: "Profile",
    }
  )
);
