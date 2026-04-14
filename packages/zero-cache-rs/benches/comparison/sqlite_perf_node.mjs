/**
 * Head-to-head benchmark: Node.js (better-sqlite3) SQLite operations.
 *
 * Run this AFTER the Rust benchmarks to compare.
 * Usage: node benches/comparison/sqlite_perf_node.mjs
 *
 * Tests the same operations as sqlite_perf.rs:
 * 1. Change log writes (replicator hot path)
 * 2. Change log reads (IVM advance hot path)
 * 3. Row reads (hydration)
 * 4. Operator storage CRUD
 * 5. Batch serialization (structured clone proxy)
 */

import { createRequire } from 'node:module';
const require = createRequire(import.meta.url);
// Use the project's SQLite — try zero-sqlite3 first, fall back to better-sqlite3
let Database;
try {
  Database = require('@rocicorp/zero-sqlite3');
} catch {
  try {
    Database = require('better-sqlite3');
  } catch {
    console.error('Neither @rocicorp/zero-sqlite3 nor better-sqlite3 found. Run from monorepo root.');
    process.exit(1);
  }
}
import { performance } from 'node:perf_hooks';
import { randomUUID } from 'node:crypto';
import { tmpdir } from 'node:os';
import path from 'node:path';

function median(arr) {
  const sorted = [...arr].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

function runBench(name, iterations, fn) {
  // Warmup
  for (let i = 0; i < Math.min(10, iterations); i++) fn();

  const times = [];
  for (let i = 0; i < iterations; i++) {
    const start = performance.now();
    fn();
    times.push((performance.now() - start) * 1_000_000); // ns
  }

  const med = median(times);
  const unit = med > 1_000_000 ? `${(med / 1_000_000).toFixed(2)} ms` :
               med > 1_000 ? `${(med / 1_000).toFixed(2)} µs` :
               `${med.toFixed(0)} ns`;
  console.log(`${name.padEnd(40)} ${unit.padStart(12)}`);
  return med;
}

// Setup
function setupReplicaDb() {
  const dbPath = path.join(tmpdir(), `bench-${randomUUID()}.db`);
  const db = new Database(dbPath);
  db.pragma('journal_mode = WAL');
  db.pragma('synchronous = NORMAL');
  db.pragma('cache_size = -8000');

  db.exec(`
    CREATE TABLE "_zero.replicationState" (
      stateVersion TEXT NOT NULL,
      lock INTEGER PRIMARY KEY DEFAULT 1 CHECK (lock=1)
    );
    INSERT INTO "_zero.replicationState" (stateVersion) VALUES ('00');

    CREATE TABLE "_zero.changeLog2" (
      "stateVersion" TEXT NOT NULL,
      "pos" INT NOT NULL,
      "table" TEXT NOT NULL,
      "rowKey" TEXT NOT NULL,
      "op" TEXT NOT NULL,
      "backfillingColumnVersions" TEXT DEFAULT '{}',
      PRIMARY KEY("stateVersion", "pos")
    );
    CREATE INDEX "_zero.changeLog2_table_rowKey"
      ON "_zero.changeLog2" ("table", "rowKey");

    CREATE TABLE "user" (
      id TEXT PRIMARY KEY,
      name TEXT,
      email TEXT,
      age INTEGER,
      bio TEXT,
      created_at TEXT,
      updated_at TEXT,
      _0_version TEXT
    );
  `);

  // Seed 10k rows
  const insert = db.prepare(
    'INSERT INTO "user" (id, name, email, age, bio, created_at, updated_at, _0_version) VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
  );
  const tx = db.transaction(() => {
    for (let i = 0; i < 10_000; i++) {
      insert.run(
        `user-${String(i).padStart(5, '0')}`,
        `User ${i}`,
        `user${i}@example.com`,
        20 + (i % 50),
        `Bio for user ${i} with some extra text to simulate realistic row sizes`,
        '2024-01-01T00:00:00Z',
        '2024-06-15T12:00:00Z',
        '00'
      );
    }
  });
  tx();
  return db;
}

function setupOperatorDb() {
  const db = new Database(':memory:');
  db.pragma('journal_mode = OFF');
  db.pragma('synchronous = OFF');
  db.pragma('locking_mode = EXCLUSIVE');
  db.exec(`
    CREATE TABLE storage (
      clientGroupID TEXT,
      op NUMBER,
      key TEXT,
      val TEXT,
      PRIMARY KEY(clientGroupID, op, key)
    );
  `);
  return db;
}

console.log('=== Node.js (better-sqlite3) Benchmarks ===\n');

// Benchmark 1: Change log writes
{
  console.log('--- changelog_writes ---');
  for (const batchSize of [1, 10, 100, 1000]) {
    const db = setupReplicaDb();
    const insertStmt = db.prepare(
      `INSERT OR REPLACE INTO "_zero.changeLog2"
       ("stateVersion","pos","table","rowKey","op","backfillingColumnVersions")
       VALUES (?,?,?,?,?,'{}')`
    );
    const updateState = db.prepare(
      `UPDATE "_zero.replicationState" SET stateVersion = ?`
    );
    let version = 1;

    const writeTx = db.transaction(() => {
      const v = version.toString(16).padStart(8, '0');
      for (let i = 0; i < batchSize; i++) {
        insertStmt.run(v, i, 'user', `{"id":"user-${String(i).padStart(5, '0')}"}`, 's');
      }
      updateState.run(v);
      version++;
    });

    runBench(`batch_${batchSize}`, batchSize > 100 ? 100 : 500, writeTx);
    db.close();
  }
}

// Benchmark 2: Change log reads
{
  console.log('\n--- changelog_reads ---');
  const db = setupReplicaDb();

  // Insert 1000 changes across 100 versions
  const insertStmt = db.prepare(
    `INSERT INTO "_zero.changeLog2"
     ("stateVersion","pos","table","rowKey","op","backfillingColumnVersions")
     VALUES (?,?,?,?,?,'{}')`
  );
  const tx = db.transaction(() => {
    for (let v = 1; v <= 100; v++) {
      const version = v.toString(16).padStart(8, '0');
      for (let pos = 0; pos < 10; pos++) {
        insertStmt.run(version, pos, 'user', `{"id":"user-${String(v * 10 + pos).padStart(5, '0')}"}`, 's');
      }
    }
  });
  tx();

  const readAll = db.prepare(
    `SELECT "stateVersion","pos","table","rowKey","op"
     FROM "_zero.changeLog2"
     WHERE "stateVersion" > ?
     ORDER BY "stateVersion","pos"`
  );
  const readRecent = db.prepare(
    `SELECT "stateVersion","pos","table","rowKey","op"
     FROM "_zero.changeLog2"
     WHERE "stateVersion" > ?
     ORDER BY "stateVersion","pos"`
  );

  runBench('read_1000_changes', 500, () => readAll.all('00000000'));
  runBench('read_100_changes', 500, () => readRecent.all((90).toString(16).padStart(8, '0')));
  db.close();
}

// Benchmark 3: Row reads
{
  console.log('\n--- row_reads ---');
  const db = setupReplicaDb();

  const selectAll = db.prepare('SELECT id, name, email, age, bio, created_at, updated_at FROM "user"');
  const selectOne = db.prepare('SELECT id, name, email, age FROM "user" WHERE id = ?');
  const selectRange = db.prepare('SELECT id, name, email, age FROM "user" WHERE age > ? ORDER BY name LIMIT 50');

  runBench('select_all_10k_rows', 100, () => selectAll.all());
  runBench('pk_lookup_single_row', 5000, () => selectOne.get('user-05000'));
  runBench('range_query_limit_50', 1000, () => selectRange.all(30));
  db.close();
}

// Benchmark 4: Operator storage
{
  console.log('\n--- operator_storage ---');
  const db = setupOperatorDb();

  const setStmt = db.prepare(
    'INSERT OR REPLACE INTO storage (clientGroupID, op, key, val) VALUES (?, ?, ?, ?)'
  );
  const getStmt = db.prepare(
    'SELECT val FROM storage WHERE clientGroupID = ? AND op = ? AND key = ?'
  );
  const scanStmt = db.prepare(
    'SELECT key, val FROM storage WHERE clientGroupID = ? AND op = ? ORDER BY key'
  );

  runBench('set_1000_keys', 200, () => {
    for (let i = 0; i < 1000; i++) {
      setStmt.run('cg-1', 0, `key-${i}`, `{"v":${i}}`);
    }
  });

  // Pre-populate for reads
  for (let i = 0; i < 1000; i++) {
    setStmt.run('cg-1', 0, `key-${String(i).padStart(4, '0')}`, `{"v":${i}}`);
  }

  runBench('get_1000_keys', 200, () => {
    for (let i = 0; i < 1000; i++) {
      getStmt.get('cg-1', 0, `key-${i}`);
    }
  });

  runBench('scan_1000_keys', 200, () => {
    scanStmt.all('cg-1', 0);
  });
  db.close();
}

// Benchmark 5: JSON serialization (proxy for structured clone cost)
{
  console.log('\n--- serialization ---');

  const makeRow = (n) => {
    const row = {};
    for (let i = 0; i < n; i++) row[`col_${i}`] = `value_${i}`;
    return row;
  };

  const makeBatch = () => {
    return Array.from({ length: 20 }, (_, i) => ({
      type: 'add',
      queryID: 'q1',
      table: 'user',
      rowKey: { id: `user-${i}` },
      row: makeRow(10),
    }));
  };

  const row10 = makeRow(10);
  const row50 = makeRow(50);
  const batch = makeBatch();

  runBench('row_10_cols_json_stringify', 10000, () => JSON.stringify(row10));
  runBench('row_50_cols_json_stringify', 10000, () => JSON.stringify(row50));
  runBench('row_10_cols_json_parse', 10000, () => JSON.parse(JSON.stringify(row10)));
  runBench('batch_20_row_changes_stringify', 5000, () => JSON.stringify(batch));

  // Structured clone (the actual IPC mechanism)
  runBench('batch_20_structured_clone', 5000, () => structuredClone(batch));
}

// Benchmark 6: Memory baseline
{
  console.log('\n--- memory ---');
  const before = process.memoryUsage();
  console.log(`RSS: ${(before.rss / 1024 / 1024).toFixed(1)} MB`);
  console.log(`Heap Used: ${(before.heapUsed / 1024 / 1024).toFixed(1)} MB`);
  console.log(`Heap Total: ${(before.heapTotal / 1024 / 1024).toFixed(1)} MB`);
}

console.log('\n=== Done ===');
