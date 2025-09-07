import { OptimaDB } from "../src";
import * as Schema from "./schema";

const DB = new OptimaDB(Schema);

DB.Tables.Users.Insert({
  Email: "ilies@gmail.com",
  Password: "1234567",
  isHuman:"12345678901"
});

console.log(DB.Tables.Users.GetOne({ Password: "1234567" }));