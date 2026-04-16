# IVM Log Parity — TS ↔ Rust

> Format for IVM trace lines so TS and Rust outputs can be diff'd line-by-line.

## Rules

1. **Every operator entry/exit gets a trace line.** Prefix identifies the implementation.
2. **Same event name** on both sides.
3. **Key=value pairs**, ordered consistently, no locale-dependent formatting.
4. **Numbers are integers** when possible; floats use `.3f`.
5. **Row PKs as JSON** — `pk={"id":42}`.
6. **Change types uppercase** — `op=Add`, `op=Remove`, `op=Edit`, `op=Child`.

## Prefixes

| Side | Prefix | Example |
|---|---|---|
| TS (current) | `[TRACE ivm]` | `[TRACE ivm] Filter::push enter op=Add` |
| Rust v1 (ivm/) | `[TRACE ivm]` | (matches TS — used for diffing during transition) |
| Rust v2 (ivm_v2/) | `[TRACE ivm_v2]` | `[TRACE ivm_v2] Filter::push enter op=Add` |

## Required trace points per operator

### Filter / Skip / Exists / Take (1-input operators)

```
[TRACE ivm_v2] {Op}::push enter op={ChangeType} [key-metadata]
[TRACE ivm_v2] {Op}::push emit op={ChangeType} pk={json}
[TRACE ivm_v2] {Op}::push drop op={ChangeType} reason={text}
```

### Join / FlippedJoin (2-input operators)

```
[TRACE ivm_v2] {Op}::push_parent enter op={ChangeType}
[TRACE ivm_v2] {Op}::push_child enter op={ChangeType} rel={name}
[TRACE ivm_v2] {Op}::fetch enter constraint={json} reverse={bool}
```

### FanOut

```
[TRACE ivm_v2] FanOut::push enter op={ChangeType} downstreams={N}
[TRACE ivm_v2] FanOut::push fanout branch={i}
```

### TableSource

```
[TRACE ivm_v2] TableSource::query_rows enter sql_hash={hash} params={N}
[TRACE ivm_v2] TableSource::query_rows row id={N}
[TRACE ivm_v2] TableSource::query_rows done rows_emitted={N}
```

## Diffing recipe

```bash
# Run both with identical input, strip prefixes, diff
grep -E '\[TRACE (ivm|ivm_v2)\]' ts.log | sed 's/^\[TRACE ivm\]/[X]/' > ts.trace
grep -E '\[TRACE (ivm|ivm_v2)\]' rs.log | sed 's/^\[TRACE ivm_v2\]/[X]/' > rs.trace
diff ts.trace rs.trace | head -50
```

## Current state vs this spec

- **Filter/Skip/Take/Exists in ivm_v2**: emit basic `{Op}::push enter op=X` — partial parity.
- **TODO after all operators ported**: tighten to match this spec including PK rendering and drop/emit annotations.
