import { OptimaDB } from "../src";
import * as Schema from "./schema";

const DB = new OptimaDB(Schema);

DB.Tables.Users.Insert({
  Email: "ilies@gmail.com",
  JSON: {
    Test: "123",
  },
  Array: [123, 123, 123],
});

console.log(DB.Tables.Users.Get({ Array: { $includes: 1223 } }));
