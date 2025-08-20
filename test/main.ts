import { OptimaDB } from "../src";
import * as Schema from "./schema";

const DB = new OptimaDB(Schema);

DB.Tables.Users.Insert({
  Email: "ilies@gmail.com",
  ID: 1,
});

