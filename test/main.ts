import { OptimaDB } from "../src";
import * as Schema from "./schema";
type ModeTriple = { Disk: string; Memory: string; Hybrid: string };

const DISKDB = new OptimaDB(Schema, {
  mode: "disk",
  path: "data",
});
const MemDB = new OptimaDB(Schema, {
  mode: "memory",
});
const HybridDB = new OptimaDB(Schema, {
  mode: "hybrid",
  path: "hybrid",
});

const benchmarkValues = {
  // Insert
  insert1K: {
    Disk: "0.0 ms",
    Memory: "0.0 ms",
    Hybrid: "0.0 ms",
  } as ModeTriple,
  insert10K: {
    Disk: "0.0 ms",
    Memory: "0.0 ms",
    Hybrid: "0.0 ms",
  } as ModeTriple,
  insert100K: {
    Disk: "0.0 ms",
    Memory: "0.0 ms",
    Hybrid: "0.0 ms",
  } as ModeTriple,
  insert1M: {
    Disk: "0.0 ms",
    Memory: "0.0 ms",
    Hybrid: "0.0 ms",
  } as ModeTriple,
  insert10M: {
    Disk: "0.0 ms",
    Memory: "0.0 ms",
    Hybrid: "0.0 ms",
  } as ModeTriple,

  // Read
  read1K: { Disk: "0.0 ms", Memory: "0.0 ms", Hybrid: "0.0 ms" } as ModeTriple,
  read10K: { Disk: "0.0 ms", Memory: "0.0 ms", Hybrid: "0.0 ms" } as ModeTriple,
  read100K: {
    Disk: "0.0 ms",
    Memory: "0.0 ms",
    Hybrid: "0.0 ms",
  } as ModeTriple,
  read1M: { Disk: "0.0 ms", Memory: "0.0 ms", Hybrid: "0.0 ms" } as ModeTriple,
  read10M: { Disk: "0.0 ms", Memory: "0.0 ms", Hybrid: "0.0 ms" } as ModeTriple,

  // Read Extended
  readExt1K: {
    Disk: "0.0 ms",
    Memory: "0.0 ms",
    Hybrid: "0.0 ms",
  } as ModeTriple,
  readExt10K: {
    Disk: "0.0 ms",
    Memory: "0.0 ms",
    Hybrid: "0.0 ms",
  } as ModeTriple,
  readExt100K: {
    Disk: "0.0 ms",
    Memory: "0.0 ms",
    Hybrid: "0.0 ms",
  } as ModeTriple,
  readExt1M: {
    Disk: "0.0 ms",
    Memory: "0.0 ms",
    Hybrid: "0.0 ms",
  } as ModeTriple,
  readExt10M: {
    Disk: "0.0 ms",
    Memory: "0.0 ms",
    Hybrid: "0.0 ms",
  } as ModeTriple,

  // Update
  update1K: {
    Disk: "0.0 ms",
    Memory: "0.0 ms",
    Hybrid: "0.0 ms",
  } as ModeTriple,
  update10K: {
    Disk: "0.0 ms",
    Memory: "0.0 ms",
    Hybrid: "0.0 ms",
  } as ModeTriple,
  update100K: {
    Disk: "0.0 ms",
    Memory: "0.0 ms",
    Hybrid: "0.0 ms",
  } as ModeTriple,
  update1M: {
    Disk: "0.0 ms",
    Memory: "0.0 ms",
    Hybrid: "0.0 ms",
  } as ModeTriple,
  update10M: {
    Disk: "0.0 ms",
    Memory: "0.0 ms",
    Hybrid: "0.0 ms",
  } as ModeTriple,

  // Delete
  delete1K: {
    Disk: "0.0 ms",
    Memory: "0.0 ms",
    Hybrid: "0.0 ms",
  } as ModeTriple,
  delete10K: {
    Disk: "0.0 ms",
    Memory: "0.0 ms",
    Hybrid: "0.0 ms",
  } as ModeTriple,
  delete100K: {
    Disk: "0.0 ms",
    Memory: "0.0 ms",
    Hybrid: "0.0 ms",
  } as ModeTriple,
  delete1M: {
    Disk: "0.0 ms",
    Memory: "0.0 ms",
    Hybrid: "0.0 ms",
  } as ModeTriple,
  delete10M: {
    Disk: "0.0 ms",
    Memory: "0.0 ms",
    Hybrid: "0.0 ms",
  } as ModeTriple,
};

const RunTests = async () => {
  // 1K Inserts (already batched)
  benchmarkValues.insert1K.Disk = await timeFunction(() => {
    DISKDB.Tables.Users.Batch(() => {
      for (let i = 0; i < 1000; i++) {
        DISKDB.Tables.Users.Insert({
          ID: i,
          Email: "test" + i + "@test.com",
          Password: "password" + i,
        });
      }
    });
  });
  benchmarkValues.insert1K.Memory = await timeFunction(() => {
    MemDB.Tables.Users.Batch(() => {
      for (let i = 0; i < 1000; i++) {
        MemDB.Tables.Users.Insert({
          ID: i,
          Email: "test" + i + "@test.com",
          Password: "password" + i,
        });
      }
    });
  });
  benchmarkValues.insert1K.Hybrid = await timeFunction(() => {
    HybridDB.Tables.Users.Batch(() => {
      for (let i = 0; i < 1000; i++) {
        HybridDB.Tables.Users.Insert({
          ID: i,
          Email: "test" + i + "@test.com",
          Password: "password" + i,
        });
      }
    });
  });

  // 10K Inserts (now batched)
  benchmarkValues.insert10K.Disk = await timeFunction(() => {
    DISKDB.Tables.Users.Batch(() => {
      for (let i = 0; i < 10000; i++) {
        DISKDB.Tables.Users.Insert({
          ID: i,
          Email: "test" + i + "@test.com",
          Password: "password" + i,
        });
      }
    });
  });
  benchmarkValues.insert10K.Memory = await timeFunction(() => {
    MemDB.Tables.Users.Batch(() => {
      for (let i = 0; i < 10000; i++) {
        MemDB.Tables.Users.Insert({
          ID: i,
          Email: "test" + i + "@test.com",
          Password: "password" + i,
        });
      }
    });
  });
  benchmarkValues.insert10K.Hybrid = await timeFunction(() => {
    HybridDB.Tables.Users.Batch(() => {
      for (let i = 0; i < 10000; i++) {
        HybridDB.Tables.Users.Insert({
          ID: i,
          Email: "test" + i + "@test.com",
          Password: "password" + i,
        });
        }
    });
  });

  // 100K Inserts (now batched)
  benchmarkValues.insert100K.Disk = await timeFunction(() => {
    DISKDB.Tables.Users.Batch(() => {
      for (let i = 0; i < 100000; i++) {
        DISKDB.Tables.Users.Insert({
          ID: i,
          Email: "test" + i + "@test.com",
          Password: "password" + i,
        });
      }
    });
  });
  benchmarkValues.insert100K.Memory = await timeFunction(() => {
    MemDB.Tables.Users.Batch(() => {
      for (let i = 0; i < 100000; i++) {
        MemDB.Tables.Users.Insert({
          ID: i,
          Email: "test" + i + "@test.com",
          Password: "password" + i,
        });
      }
    });
  });
  benchmarkValues.insert100K.Hybrid = await timeFunction(() => {
    HybridDB.Tables.Users.Batch(() => {
      for (let i = 0; i < 100000; i++) {
        HybridDB.Tables.Users.Insert({
          ID: i,
          Email: "test" + i + "@test.com",
          Password: "password" + i,
        });
      }
    });
  });

  // 1M Inserts (NEW - batched)
  benchmarkValues.insert1M.Disk = await timeFunction(() => {
    DISKDB.Tables.Users.Batch(() => {
      for (let i = 0; i < 1_000_000; i++) { // Using numeric separators for readability
        DISKDB.Tables.Users.Insert({
          ID: i,
          Email: "test" + i + "@test.com",
          Password: "password" + i,
        });
      }
    });
  });
  benchmarkValues.insert1M.Memory = await timeFunction(() => {
    MemDB.Tables.Users.Batch(() => {
      for (let i = 0; i < 1_000_000; i++) {
        MemDB.Tables.Users.Insert({
          ID: i,
          Email: "test" + i + "@test.com",
          Password: "password" + i,
        });
      }
    });
  });
  benchmarkValues.insert1M.Hybrid = await timeFunction(() => {
    HybridDB.Tables.Users.Batch(() => {
      for (let i = 0; i < 1_000_000; i++) {
        HybridDB.Tables.Users.Insert({
          ID: i,
          Email: "test" + i + "@test.com",
          Password: "password" + i,
        });
      }
    });
  });

  // 10M Inserts (NEW - batched)
  benchmarkValues.insert10M.Disk = await timeFunction(() => {
    DISKDB.Tables.Users.Batch(() => {
      for (let i = 0; i < 10_000_000; i++) {
        DISKDB.Tables.Users.Insert({
          ID: i,
          Email: "test" + i + "@test.com",
          Password: "password" + i,
        });
      }
    });
  });
  benchmarkValues.insert10M.Memory = await timeFunction(() => {
    MemDB.Tables.Users.Batch(() => {
      for (let i = 0; i < 10_000_000; i++) {
        MemDB.Tables.Users.Insert({
          ID: i,
          Email: "test" + i + "@test.com",
          Password: "password" + i,
        });
      }
    });
  });
  benchmarkValues.insert10M.Hybrid = await timeFunction(() => {
    HybridDB.Tables.Users.Batch(() => {
      for (let i = 0; i < 10_000_000; i++) {
        HybridDB.Tables.Users.Insert({
          ID: i,
          Email: "test" + i + "@test.com",
          Password: "password" + i,
        });
      }
    });
  });
};


async function timeFunction<TArgs extends unknown[], TResult>(
  fn: (...args: TArgs) => TResult | Promise<TResult>,
  ...args: TArgs
): Promise<string> {
  const hasPerfNow =
    typeof globalThis !== "undefined" &&
    typeof (globalThis as any).performance !== "undefined" &&
    typeof (globalThis as any).performance.now === "function";

  const now = hasPerfNow
    ? () => (globalThis as any).performance.now()
    : () => Date.now();

  const start = now();
  const result = fn(...args);
  if (result && typeof (result as any).then === "function") {
    await (result as any);
  }

  const end = now();
  const durationMs = end - start; // Duration in milliseconds
  if (durationMs < 1000) {
    return `${durationMs.toFixed(0)} ms`;
  } else {
    const durationSeconds = durationMs / 1000;
    return `${durationSeconds.toFixed(1)}s`;
  }
}

function buildBenchmarkTable() {
  return {
    // --- Insert Operations ---
    "Insert            1K": benchmarkValues.insert1K,
    "Insert           10K": benchmarkValues.insert10K,
    "Insert          100K": benchmarkValues.insert100K,
    "Insert            1M": benchmarkValues.insert1M,
    "Insert           10M": benchmarkValues.insert10M,

    // --- Separator ---
    "────────────────────‎": {
      Disk: "────────",
      Memory: "────────",
      Hybrid: "────────",
    },

    // --- Read Operations ---
    "Read              1K": benchmarkValues.read1K,
    "Read             10K": benchmarkValues.read10K,
    "Read            100K": benchmarkValues.read100K,
    "Read              1M": benchmarkValues.read1M,
    "Read             10M": benchmarkValues.read10M,

    // --- Separator ---
    "────────────────────‎‎": {
      Disk: "────────",
      Memory: "────────",
      Hybrid: "────────",
    },

    // --- Read Extended Operations ---
    "Read Extended     1K": benchmarkValues.readExt1K,
    "Read Extended    10K": benchmarkValues.readExt10K,
    "Read Extended   100K": benchmarkValues.readExt100K,
    "Read Extended     1M": benchmarkValues.readExt1M,
    "Read Extended    10M": benchmarkValues.readExt10M,

    // --- Separator ---
    "────────────────────‎‎‎": {
      Disk: "────────",
      Memory: "────────",
      Hybrid: "────────",
    },

    // --- Update Operations ---
    "Update            1K": benchmarkValues.update1K,
    "Update           10K": benchmarkValues.update10K,
    "Update          100K": benchmarkValues.update100K,
    "Update            1M": benchmarkValues.update1M,
    "Update           10M": benchmarkValues.update10M,

    // --- Separator ---
    "────────────────────‎‎‎‎": {
      Disk: "────────",
      Memory: "────────",
      Hybrid: "────────",
    },

    // --- Delete Operations ---
    "Delete            1K": benchmarkValues.delete1K,
    "Delete           10K": benchmarkValues.delete10K,
    "Delete          100K": benchmarkValues.delete100K,
    "Delete            1M": benchmarkValues.delete1M,
    "Delete           10M": benchmarkValues.delete10M,
  } as const;
}

console.log("OptimaDB Benchmark ⚙️");
RunTests()
  .then(() => {
    console.table(buildBenchmarkTable());
  })
  .finally(() => {
    try {
      DISKDB.Close();
    } catch {}
    try {
      MemDB.Close();
    } catch {}
    try {
      HybridDB.Close();
    } catch {}
  });
