import { OptimaDB } from "./../src";
import * as Schema from "./schema";

const DB = new OptimaDB(Schema);

// Seed sample rows with varying numeric values
for (let i = 0; i < 10; i++) {
  DB.Tables.Profile.Insert({
    ID:1
  });
}


console.log(DB.Tables.Profile.Get());
