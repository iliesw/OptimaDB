### OptimaDB - Made by Inflector

Type-safe, zero-ORM wrapper over Bun/SQLite with a tiny, ergonomic API. Define your schema in TypeScript and get fully typed CRUD, rich querying, relations, and automatic migrations at startup.

### Why Optima DB

- Simple, small surface area (CRUD + typed where + relations)
- Strong TypeScript types from schema to rows and queries
- No external migration files; in-code schema is the source of truth
- Fast by default (WAL, batched transactions)

### Built With

- Bun
- SQLite
- TypeScript

## Getting Started

```sh
bun i @inflector/db
```

## Usage

```ts
import { OptimaDB } from "@inflector/db";
import * as Schema from "./schema";

const DB = new OptimaDB(Schema); // In-Memory Database
```

### Modes

- In-Memory

  ```ts
  const DB = new OptimaDB(Schema);
  ```

- In-Disk

  ```ts
  const DB = new OptimaDB(Schema, {
    mode: "disk",
    path: "data",
  });
  ```

- Hybrid - In-Memory with Disk persistence

  ```ts
  const DB = new OptimaDB(Schema, {
    mode: "hybrid",
    path: "data",
    // optional autosave tuning
    // autosave: { enabled: true, debounceMs: 1500, intervalMs: 30000 },
  });
  ```

## Schema

```ts
import { Table, Int, Email, Password } from "@inflector/db";

export const Users = Table("Users", {
  ID: Int({ default: 1, notNull: true }),
  Email: Email(),
  Password: Password(),
});
```

## CRUD

- Read

  ```ts
  // all users
  const users = DB.Tables.Users.Get();
  // one user by id
  const user = DB.Tables.Users.GetOne({ ID: 1 });
  // with operators
  const list = DB.Tables.Users.Get({ ID: { $between: [1, 100] } });
  ```

- Insert

  ```ts
  DB.Tables.Users.Insert({ ID: 1, Email: "a@b.co", Password: "secret" });
  ```

- Update

  ```ts
  DB.Tables.Users.Update({ Password: "new" }, { Email: { $like: "a@%" } });
  ```

- Delete

  ```ts
  DB.Tables.Users.Delete({ ID: { $in: [2, 3] } });
  ```

## JSON Operators

OptimaDB supports powerful JSON operators for querying JSON fields:

### Basic JSON Operations

```ts
// Check if JSON contains a value
const admins = DB.Tables.Users.Get({ 
  metadata: { $json_contains: "admin" } 
});

// Extract and compare JSON path values
const darkThemeUsers = DB.Tables.Users.Get({ 
  preferences: { $json_extract: { path: "$.theme", value: "dark" } } 
});

// Check if JSON has a specific key
const usersWithRole = DB.Tables.Users.Get({ 
  metadata: { $json_has_key: "$.role" } 
});
```

### Advanced JSON Operations

```ts
// Check JSON type
const arrayFields = DB.Tables.Users.Get({ 
  tags: { $json_type: { path: "$", type: "array" } } 
});

// Check JSON array length
const usersWithManyTags = DB.Tables.Users.Get({ 
  tags: { $json_length: { path: "$", operator: "gt", length: 3 } } 
});

// Search within JSON
const developers = DB.Tables.Users.Get({ 
  metadata: { $json_search: { path: "$.description", query: "developer" } } 
});

// Check if JSON contains all/any specified keys
const completeProfiles = DB.Tables.Users.Get({ 
  metadata: { $json_contains_all: ["$.role", "$.department", "$.level"] } 
});

const partialProfiles = DB.Tables.Users.Get({ 
  metadata: { $json_contains_any: ["$.role", "$.department"] } 
});

// Validate JSON format
const validJsonUsers = DB.Tables.Users.Get({ 
  metadata: { $json_valid: true } 
});
```

### Complex JSON Queries

```ts
const complexQuery = {
  $and: [
    { metadata: { $json_extract: { path: "$.role", value: "admin" } } },
    { preferences: { $json_length: { path: "$.features", operator: "gte", length: 5 } } },
    { tags: { $json_contains_any: ["javascript", "typescript"] } }
  ]
};

const results = DB.Tables.Users.Get(complexQuery);
```

## RoadMap

- Additional examples and guides
- Query builder helpers and utilities
- More field helpers and constraints

## Licence

This project is licensed under the Inflector Public Attribution License (IPAL) v1.0. See `LICENSE` for full terms. In short:

- You can use, modify, and distribute, including commercially
- You must attribute: "This work is based on OptimaDB by Inflector"
- You may not misrepresent the origin or claim it as your own
- Trademarks/names are not granted beyond attribution
