# New pattern: lenient UTF-8 decode for pgoutput strings

## Category
D (Data) / E (Wire).

## Where used
- `packages/zero-cache/src/services/change-source/pg/logical-replication/binary-reader.ts:3-4`
- `packages/zero-cache/src/services/change-source/pg/logical-replication/binary-reader.ts:52-54` (`decodeText`)
- Invoked by `BinaryReader.readString()` at `binary-reader.ts:38-50`

## TS form
```ts
// should not use { fatal: true } because ErrorResponse can use invalid utf8 chars
const textDecoder = new TextDecoder();

decodeText(strBuf: Uint8Array) {
  return textDecoder.decode(strBuf);
}

readString() {
  const endIdx = this.#b.indexOf(0x00, this.#p);
  if (endIdx < 0) throw Error('unexpected end of message');
  const strBuf = this.#b.subarray(this.#p, endIdx);
  this.#p = endIdx + 1;
  return this.decodeText(strBuf);
}
```

The comment is load-bearing: the PG `ErrorResponse` message can contain invalid
UTF-8 bytes, and the decoder must not throw — it must substitute U+FFFD.

## Proposed Rust form
```rust
// Lenient UTF-8: never panics, substitutes U+FFFD for invalid bytes.
// std::str::from_utf8 is strict; use String::from_utf8_lossy which matches
// TextDecoder's default (non-fatal) behaviour.
fn read_c_string(&mut self) -> Result<String, ProtocolError> {
    let start = self.pos;
    let end = self.buf[start..]
        .iter()
        .position(|&b| b == 0)
        .ok_or(ProtocolError::UnexpectedEndOfMessage)?;
    let slice = &self.buf[start..start + end];
    self.pos = start + end + 1;
    Ok(String::from_utf8_lossy(slice).into_owned())
}
```

If the caller can tolerate `Cow<str>` and the buffer lives long enough, skip
`.into_owned()` to avoid the allocation when the input is already valid UTF-8.

For performance-sensitive hot paths, `simdutf8::basic::from_utf8` followed by
`String::from_utf8_lossy` on failure is ~2–4× faster than the stdlib validator
on modern x86/arm (crate docs).

## Classification
Idiom-swap — a one-call replacement, but the semantics are not the stdlib
default, so it must be named explicitly.

## Caveats
- `std::str::from_utf8` returns `Err` on invalid bytes; do NOT use it here.
- The guide's D4 row ("Binary data handling") cites `bytes::Buf`, `byteorder`,
  `hex`, but does not cover lenient UTF-8 decoding. This decision matters
  because pgoutput's `Relation.columnName`, `Relation.schemaName`, and
  `Message.prefix` are NUL-terminated C strings whose encoding PG does not
  guarantee to be valid UTF-8 in all edge cases (especially ErrorResponse).
- Postgres-protocol's own `read_cstr` helper
  (`postgres_protocol::message::backend::read_value`) returns `Bytes`, not a
  `String`, for this exact reason — callers choose the decoding strategy.

## Citation
- `String::from_utf8_lossy` — https://doc.rust-lang.org/std/string/struct.String.html#method.from_utf8_lossy
- `simdutf8` crate — https://docs.rs/simdutf8/latest/simdutf8/
- postgres-protocol read helpers — https://docs.rs/postgres-protocol/latest/postgres_protocol/message/backend/index.html
