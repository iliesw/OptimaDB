import { OptimaDB } from "../src";
import * as Schema from "./schema";

const DB = new OptimaDB(Schema);

DB.Tables.Users.Insert({
Email:"ilies@gmail.com",
Password:"123456",
});


console.log(
  DB.Tables.Users.Get(
    { Password: "123456" },
    {
      Extend: "Profile",
    }
  )
);
