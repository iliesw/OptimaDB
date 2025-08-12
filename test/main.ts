import { OptimaDB } from "../src";
import * as Schema from "./schema";

const DB = new OptimaDB(Schema,{
  mode:"Hybrid",
  path:"data"
});
// DB.Tables.Users.Insert({
//   ID:1,
//   Age:18,
//   Email:"ilies@test.com",
//   Password:"password"
// })
console.dir(DB.Tables.Users.GetOne())
