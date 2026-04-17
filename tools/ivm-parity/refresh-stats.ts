/**
 * Live-refresh `sqlite_stat1` / `sqlite_stat4` on both TS and RS
 * replicas without restarting the caches.
 *
 * Why: `migration-lite.ts:145` runs `ANALYZE main` only during the
 * migration that introduces a schema version; once replicas are caught
 * up, ongoing replication adds rows without re-ANALYZE-ing. If the two
 * caches were initialized at different times (different data volumes),
 * their stats drift, and the planner (invoked by both TS
 * `pipeline-driver.ts` and RS `rust-pipeline-driver-v2.ts` via
 * `createSQLiteCostModel` + `planQuery`) can pick different plans for
 * identical ASTs — surfacing as harness divergences that are NOT IVM
 * bugs.
 *
 * SQLite WAL mode permits a writer concurrent with readers, so `ANALYZE`
 * succeeds over a second connection while the cache holds the primary
 * one. Used as a pre-sweep setup step.
 */
import {Database} from '../../packages/zqlite/src/db.ts';
import {consoleLogSink, LogContext} from '@rocicorp/logger';

const paths = process.argv.slice(2);
if (paths.length === 0) {
  paths.push('/tmp/xyne-lite-ts.db', '/tmp/xyne-lite-rs.db');
}

for (const p of paths) {
  const lc = new LogContext('info', undefined, consoleLogSink);
  const db = new Database(lc, p);
  const start = Date.now();
  try {
    db.exec('ANALYZE');
    console.log(`[refresh-stats] ANALYZE ${p} ok in ${Date.now() - start}ms`);
  } catch (e: any) {
    console.error(`[refresh-stats] ANALYZE ${p} failed:`, e.code, e.message);
    process.exitCode = 1;
  }
}
