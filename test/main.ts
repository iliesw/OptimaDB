import { OptimaDB } from "../src";
import * as Schema from "./schema";

const DB = new OptimaDB(Schema);

const User = DB.Tables.Users.Insert({
  Email: "ilies@gmail.com",
  Password: "1234567",
  JSON: {
    X: 123,
    Y: 1234,
  },
});
console.log(new Date("x").toString())