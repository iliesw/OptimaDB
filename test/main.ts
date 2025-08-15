import { OptimaDB } from "../src";
import * as Schema from "./schema";

const DB = new OptimaDB(Schema,{
  mode:"Hybrid",
  path:"data"
});

DB.Tables.Comments.Insert({
  ID:1
})

console.dir(DB.Tables.Comments.GetOne())
