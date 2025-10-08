import { OptimaDB, TableToSQL } from "./../src";
import * as Schema from "./schema";
const DB = new OptimaDB(Schema);

console.log(TableToSQL(Schema.Users,"a"))

console.log(DB.Tables.Users.Insert({
  Email:"hello@p.com"
},true))
console.log(DB.Tables.Users.Get({
  Dates:{
    $eafter:new Date(Date.now()-100)
  }
}))