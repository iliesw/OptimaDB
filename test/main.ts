import { OptimaDB } from "@inflector/db";
import * as Schema from "./schema";
const DB = new OptimaDB(Schema);

DB.Tables.Users.Insert({
  Email: "ilies@gmail.com",
});

const User = DB.Tables.Users.Get({
  $or: [
    {
      Email: "ilies2@gmail.com",
    },
    {
      JSON: {
        $eq: {
          X: 1,
          Y: 2,
        },
      },
    },
  ],
});

console.log(User)