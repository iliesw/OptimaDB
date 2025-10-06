import { OptimaDB } from "./../src";
import * as Schema from "./schema";
const DB = new OptimaDB(Schema,{
  mode:"Disk",
  path:"a"
});


DB.Tables.Users.Get({
  Dates:{
  }
})