import { OptimaDB } from "../src";
import * as Schema from "./schema";

const DB = new OptimaDB(Schema,{
  mode:"Disk",
  path:"Data"
});

console.log(DB.Tables.Users.Get())

DB.Tables.Users.Insert({
  Email: "ilies@gmail.com",
});

