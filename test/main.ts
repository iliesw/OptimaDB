import { OptimaDB } from "@inflector/db";
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

DB.Tables.Users.Upsert({
  ID: User.ID,
  Email: "ilies123@gmail.com",
  Password: "1234567",
  JSON: {
    X: 1234,
    Y: 1234,
  },
});
console.log(
  DB.Tables.Users.GetOne({
    Password: "1234567",
  })
);
