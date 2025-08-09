import { OptimaDB } from "./../src";
import * as Schema from "./schema";

const DB = new OptimaDB(Schema, "data");

DB.Tables.Profile.Insert({
  ID: 0,
  Bio: { name: "John", age: 30, car: null },
  Likes:500,
  UserID:1,
  Win:[1,2,3,4,5,6,7,8,9]
});

console.log(
  DB.Tables.Profile.Get(
    {},
    {
      Limit: 5,
    }
  )
);
