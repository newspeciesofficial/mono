import {describe, expect, test} from 'vitest';
import {
  compareValues,
  makeComparator,
  normalizeUndefined,
  valuesEqual,
} from '../../../zql/src/ivm/data.ts';
import type {Row} from '../../../zero-protocol/src/data.ts';
import type {Ordering} from '../../../zero-protocol/src/ast.ts';
import {diff, loadShadowNative} from './index.ts';

/**
 * Differential-test the Rust port of `packages/zql/src/ivm/data.ts` against
 * the stock TypeScript implementation. Every test runs both sides on the
 * same input and asserts the output matches via the {@link diff} utility.
 *
 * Coverage map:
 *   - `compareValues` — null ordering, type-correct paths, error path
 *   - `valuesEqual` — null/undefined non-equality, value equality
 *   - `normalizeUndefined` — undefined ↔ null collapse
 *   - `makeComparator` — single-field, multi-field, `desc`, `reverse`
 *
 * If the native module isn't built the suite is skipped (no shadow
 * binary == nothing to diff). Build with:
 *   cd packages/zero-cache-rs/crates/shadow-ffi
 *   npx napi build --platform --release --js index.js --dts index.d.ts
 */

function tryNative() {
  try {
    return loadShadowNative();
  } catch {
    return undefined;
  }
}

const native = tryNative();
const it = native ? test : test.skip;

describe('ivm/data shadow: compareValues', () => {
  // Curated value matrix exercising every TS branch.
  const VALUES_BY_TYPE: Array<{
    label: string;
    pairs: Array<[unknown, unknown]>;
  }> = [
    {
      label: 'numbers',
      pairs: [
        [1, 2],
        [10, 9],
        [-5, 5],
        [0, 0],
        [3.14, 3.15],
      ],
    },
    {
      label: 'strings',
      pairs: [
        ['a', 'b'],
        ['ä', 'a'],
        ['', ''],
        ['hello', 'help'],
        ['utf-8', 'utf-8'],
      ],
    },
    {
      label: 'booleans',
      pairs: [
        [false, true],
        [true, false],
        [true, true],
        [false, false],
      ],
    },
    {
      label: 'nullish',
      pairs: [
        [null, null],
        [null, 'x'],
        [42, null],
        // Both `null` and `undefined` collapse — pick the variant TS handles
        // via the `_opt` form so we don't push undefined through JSON.
      ],
    },
  ];

  for (const {label, pairs} of VALUES_BY_TYPE) {
    it(`matches TS for ${label}`, () => {
      for (const [a, b] of pairs) {
        const ts = compareValues(a as never, b as never);
        const rs = native!.ivm_data_compare_values_opt(a, b);
        const r = diff(
          `compareValues(${JSON.stringify(a)}, ${JSON.stringify(b)})`,
          normaliseSign(ts),
          rs,
        );
        expect(r.ok).toBe(true);
      }
    });
  }

  it('treats undefined and null as equal (compareValues)', () => {
    const ts = compareValues(undefined as never, null as never);
    const rs = native!.ivm_data_compare_values_opt(undefined, null);
    expect(rs).toBe(normaliseSign(ts));
    expect(rs).toBe(0);
  });

  it('throws on cross-type comparison', () => {
    expect(() => compareValues(1 as never, 'x' as never)).toThrow();
    expect(() => native!.ivm_data_compare_values(1, 'x')).toThrow();
  });
});

describe('ivm/data shadow: valuesEqual', () => {
  it('matches TS across nullish + value cases', () => {
    const cases: Array<[unknown, unknown]> = [
      [null, null],
      [null, 'x'],
      ['x', null],
      ['x', 'x'],
      [42, 42],
      [1, 2],
      [false, false],
      [true, false],
    ];
    for (const [a, b] of cases) {
      const ts = valuesEqual(a as never, b as never);
      const rs = native!.ivm_data_values_equal(a, b);
      expect(rs).toBe(ts);
    }
  });

  it('handles undefined', () => {
    expect(valuesEqual(undefined as never, 'x' as never)).toBe(false);
    expect(native!.ivm_data_values_equal(undefined, 'x')).toBe(false);
  });
});

describe('ivm/data shadow: normalizeUndefined', () => {
  it('null and undefined both fold to null', () => {
    expect(normalizeUndefined(undefined as never)).toBe(null);
    expect(native!.ivm_data_normalize_undefined(undefined)).toBe(null);
    expect(normalizeUndefined(null as never)).toBe(null);
    expect(native!.ivm_data_normalize_undefined(null)).toBe(null);
  });

  it('passes through non-nullish', () => {
    expect(native!.ivm_data_normalize_undefined('x')).toBe('x');
    expect(native!.ivm_data_normalize_undefined(42)).toBe(42);
    expect(native!.ivm_data_normalize_undefined(false)).toBe(false);
  });
});

describe('ivm/data shadow: makeComparator', () => {
  type Case = {
    label: string;
    order: Ordering;
    reverse: boolean;
    rows: Array<[Row, Row]>;
  };

  const cases: Case[] = [
    {
      label: 'single asc field, integer ids',
      order: [['id', 'asc']] as Ordering,
      reverse: false,
      rows: [
        [{id: 1}, {id: 2}],
        [{id: 5}, {id: 5}],
        [{id: 10}, {id: 9}],
      ],
    },
    {
      label: 'single desc field flips',
      order: [['id', 'desc']] as Ordering,
      reverse: false,
      rows: [
        [{id: 1}, {id: 2}],
        [{id: 5}, {id: 5}],
      ],
    },
    {
      label: 'reverse=true flips entire result',
      order: [['id', 'asc']] as Ordering,
      reverse: true,
      rows: [[{id: 1}, {id: 2}]],
    },
    {
      label: 'multi-field falls through tie',
      order: [
        ['group', 'asc'],
        ['id', 'asc'],
      ] as Ordering,
      reverse: false,
      rows: [
        [
          {group: 'x', id: 1},
          {group: 'x', id: 2},
        ],
        [
          {group: 'x', id: 1},
          {group: 'y', id: 0},
        ],
      ],
    },
    {
      label: 'mixed asc/desc',
      order: [
        ['group', 'asc'],
        ['id', 'desc'],
      ] as Ordering,
      reverse: false,
      rows: [
        [
          {group: 'x', id: 2},
          {group: 'x', id: 1},
        ],
        [
          {group: 'x', id: 1},
          {group: 'y', id: 99},
        ],
      ],
    },
  ];

  for (const c of cases) {
    it(`matches TS for: ${c.label}`, () => {
      const cmp = makeComparator(c.order, c.reverse);
      for (const [a, b] of c.rows) {
        const ts = cmp(a, b);
        const rs = native!.ivm_data_compare_rows(
          c.order as Array<[string, 'asc' | 'desc']>,
          c.reverse,
          a as Record<string, unknown>,
          b as Record<string, unknown>,
        );
        const got = diff(
          `compare(${JSON.stringify(a)}, ${JSON.stringify(b)})`,
          normaliseSign(ts),
          rs,
        );
        expect(got.ok).toBe(true);
      }
    });
  }
});

/**
 * TS comparators return arbitrary signed integers; Rust returns -1/0/1.
 * Normalise both to -1/0/1 so the diff comparator only cares about sign.
 */
function normaliseSign(n: number): number {
  if (n < 0) return -1;
  if (n > 0) return 1;
  return 0;
}
