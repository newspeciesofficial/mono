/**
 * Output diff for shadow comparisons.
 *
 * Compares the TS implementation's output to the Rust shadow's output
 * using a structural deep-equal that's tolerant of the obvious wire
 * mismatches (BigInt ↔ string, Buffer ↔ array, undefined-vs-missing
 * fields). Returns a {@link DiffResult}; the harness decides whether to
 * log, throw, or push to a metric.
 *
 * The diff comparator is deliberately ZQL-aware where it has to be —
 * notably for row patches that may arrive in different orders depending on
 * iterator traversal. Add normalisers per type as you shadow more
 * functions; keep them isolated in {@link normalisers}.
 */

export type DiffResult =
  | {ok: true}
  | {ok: false; path: string; ts: unknown; rust: unknown; reason: string};

/**
 * Normalisers translate "equivalent but differently-shaped" representations
 * into a canonical form before equality. Add cases here as new shadow
 * functions hit them — never reach into `eq` to special-case in place.
 */
const normalisers: Array<(v: unknown) => unknown> = [
  // BigInt → decimal string. JSON has no BigInt; Rust returns BigInt via
  // napi-rs but TS code may produce a `bigint` or already a string. Coerce
  // both sides to decimal-string for comparison.
  v => (typeof v === 'bigint' ? v.toString() : v),
  // Buffer / Uint8Array → number[] for stable structural compare.
  v => (Buffer.isBuffer(v) || v instanceof Uint8Array ? Array.from(v) : v),
];

function normalise(v: unknown): unknown {
  let out = v;
  for (const n of normalisers) out = n(out);
  return out;
}

function eq(ts: unknown, rust: unknown, path: string): DiffResult {
  const a = normalise(ts);
  const b = normalise(rust);

  if (a === b) return {ok: true};

  // Both nullish but not strictly equal (null vs undefined): treat as
  // equal — TS often returns `undefined` where Rust serialises to `null`.
  if (a == null && b == null) return {ok: true};

  if (typeof a !== typeof b) {
    return {
      ok: false,
      path,
      ts: a,
      rust: b,
      reason: `type mismatch: ts=${typeof a}, rust=${typeof b}`,
    };
  }

  if (Array.isArray(a) && Array.isArray(b)) {
    if (a.length !== b.length) {
      return {
        ok: false,
        path,
        ts: a,
        rust: b,
        reason: `array length mismatch: ts=${a.length}, rust=${b.length}`,
      };
    }
    for (let i = 0; i < a.length; i++) {
      const r = eq(a[i], b[i], `${path}[${i}]`);
      if (!r.ok) return r;
    }
    return {ok: true};
  }

  if (
    typeof a === 'object' &&
    a !== null &&
    typeof b === 'object' &&
    b !== null
  ) {
    const ka = Object.keys(a as Record<string, unknown>).sort();
    const kb = Object.keys(b as Record<string, unknown>).sort();
    if (ka.join(',') !== kb.join(',')) {
      return {
        ok: false,
        path,
        ts: ka,
        rust: kb,
        reason: `key set mismatch`,
      };
    }
    for (const k of ka) {
      const r = eq(
        (a as Record<string, unknown>)[k],
        (b as Record<string, unknown>)[k],
        path === '' ? k : `${path}.${k}`,
      );
      if (!r.ok) return r;
    }
    return {ok: true};
  }

  return {ok: false, path, ts: a, rust: b, reason: 'value mismatch'};
}

/** Public diff entry point. `label` is used in log lines / errors. */
export function diff(label: string, ts: unknown, rust: unknown): DiffResult {
  const r = eq(ts, rust, '');
  if (!r.ok) {
    // Best-effort console line. Production wrappers should also push to
    // OTEL / structured logging, but printing here means smoke tests pick
    // up failures without ceremony.
    // eslint-disable-next-line no-console
    console.error(
      `[shadow-diff] ${label} divergence at "${r.path}" (${r.reason})\n  ts:   ${JSON.stringify(r.ts)}\n  rust: ${JSON.stringify(r.rust)}`,
    );
  }
  return r;
}
