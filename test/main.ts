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

DB.Tables.Profile.Insert({
  ID:1,
  Bio:{Field123:123},
  UserID:User.ID
})
console.log(
  DB.Tables.Users.GetOne({
  Email:"ilies@gmail.com"
},{
  Extend:"Profile"
})?.$Profile.at(0)?.Bio.Field123

)