// oxlint-disable no-console

/**
 * Benchmark: compareUTF8 vs Intl.Collator vs JS < operator
 *
 * Run with: node tsx bench-utf8-collate.ts
 *
 * compareUTF8 (npm:compare-utf8) compares strings by their UTF-8 byte values,
 * equivalent to PostgreSQL's COLLATE "ucs_basic" / SQLite default collation.
 *
 * If compare-utf8 is installed (npm install), it will be used directly.
 * Otherwise an equivalent TextEncoder-based fallback is used.
 */

// ---------------------------------------------------------------------------
// Implementations
// ---------------------------------------------------------------------------

import { compareUTF8 } from 'compare-utf8';
import { bench, run, summary } from 'mitata';
import { createLocaleComparator } from './create-locale-comparator.ts';

const compareEnUS = createLocaleComparator('en-US');
const compareDeDE = createLocaleComparator('de-DE');
const compareEnGB = createLocaleComparator('en-GB');
const compareFrFR = createLocaleComparator('fr-FR');
const compareSvSE = createLocaleComparator('sv-SE');



/**
 * Raw JS operator - compares UTF-16 code units. Same as compareUTF8 for BMP
 * characters, differs for surrogate pairs (chars > U+FFFF).
 */
function compareJS(a: string, b: string): number {
  return a === b ? 0 : a < b ? -1 : 1;
}

const collatorEnUS = new Intl.Collator('en-US');
const compareIntl = (a: string, b: string): number => collatorEnUS.compare(a, b);

// ---------------------------------------------------------------------------
// Interesting correctness cases — where the methods disagree
// ---------------------------------------------------------------------------

const cases: ReadonlyArray<readonly [string, string]> = [
  // 1. Case: uppercase sorts before lowercase in UTF-8, locale groups them
  ['Apple', 'banana'],
  ['Zebra', 'apple'],

  // 2. Accented chars: é (U+00E9=233) > z (122) in UTF-8; locale puts é near e
  ['zoo', 'élan'],
  ['zebra', 'éclair'],

  // 3. German sharp-s (U+00DF=223): UTF-8 puts it after ASCII; de_DE treats as "ss"
  ['strasse', 'straße'],

  // 4. Ligature ae (U+00E6): UTF-8 puts it after z; locale puts it near "ae"
  ['zoo', 'ænema'],

  // 5. Chars outside BMP (surrogate pairs in JS) - emoji and rare CJK
  //    UTF-8 and UTF-16 code unit order can differ here
  ['\uD83D\uDE00', '\uD83D\uDE01'], // U+1F600 vs U+1F601
  ['\u{1F600}', '\u{1F30D}'], // U+1F600 vs U+1F30D (first is greater)

  // 6. Locale ignores punctuation as secondary weight
  ['co-op', 'coop'],
  ['re-sort', 'resort'],

  // 7. Numbers in strings
  ['item10', 'item9'], // UTF-8: '1' < '9', so item10 < item9

  // 8. Null bytes / control chars
  ['a\u0000b', 'aa'],
];

console.log('=== Correctness: where methods disagree ===\n');
console.log(
  'Pair'.padEnd(35),
  'compareUTF8'.padEnd(14),
  'compareJS'.padEnd(12),
  'Intl.Collator'.padEnd(16),
  'compareEnUS',
);
console.log('-'.repeat(80));

function sign(n: number): '<' | '>' | '=' {
  return n < 0 ? '<' : n > 0 ? '>' : '=';
}

function escapeForDisplay(s: string): string {
  let out = '';
  for (let i = 0; i < s.length; i++) {
    const cp = s.charCodeAt(i);
    if ((cp >= 0 && cp <= 31) || cp === 127) {
      out += `\\u${cp.toString(16).padStart(4, '0')}`;
    } else {
      out += s[i];
    }
  }
  return out;
}

for (const [a, b] of cases) {
  const utf8 = sign(compareUTF8(a, b));
  const js = sign(compareJS(a, b));
  const intl = sign(compareIntl(a, b));
  const comp = sign(compareEnUS(a, b));
  const compMismatch = comp !== intl ? ' MISMATCH' : '';

  const pair = `"${escapeForDisplay(a)}" vs "${escapeForDisplay(b)}"`;
  console.log(
    pair.padEnd(35),
    utf8.padEnd(14),
    js.padEnd(12),
    intl.padEnd(16),
    comp + compMismatch,
  );
}

// ---------------------------------------------------------------------------
// Performance benchmark
// ---------------------------------------------------------------------------

const ITERATIONS = 100;
const STR_LEN = 10_00;
const SEED = 42;

// Simple seeded PRNG (mulberry32)
function mulberry32(seed: number): () => number {
  return function () {
    seed |= 0;
    seed = (seed + 0x6d2b79f5) | 0;
    let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function makeStrings(
  n: number,
  len: number,
  charset: string,
  seed: number,
): string[] {
  const rand = mulberry32(seed);
  const chars = charset.split('');
  const out: string[] = [];
  for (let i = 0; i < n; i++) {
    let s = '';
    for (let j = 0; j < len; j++) {
      s += chars[Math.floor(rand() * chars.length)];
    }
    out.push(s);
  }
  return out;
}

const ASCII_CHARS =
  'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
const UNICODE_CHARS =
  ASCII_CHARS +
  '\u00e0\u00e1\u00e2\u00e3\u00e4\u00e5\u00e6\u00e7\u00e8\u00e9\u00ea\u00eb\u00ec\u00ed\u00ee\u00ef\u00f0\u00f1\u00f2\u00f3\u00f4\u00f5\u00f6\u00f8\u00f9\u00fa\u00fb\u00fc\u00fd\u00fe\u00ff\u00df\u0153\u00e6';

const asciiStrings = makeStrings(ITERATIONS, STR_LEN, ASCII_CHARS, SEED);
const unicodeStrings = makeStrings(ITERATIONS, STR_LEN, UNICODE_CHARS, SEED);

const benchmarkInputs: ReadonlyArray<readonly [string, ReadonlyArray<string>]> = [
  ['ASCII', asciiStrings],
  ['Unicode', unicodeStrings],
];

for (const input of benchmarkInputs) {
  const label: string = input[0];
  const strings: ReadonlyArray<string> = input[1];
  summary(() => {
    bench(`compareUTF8 (${label})`, () => {
      let sink = 0;
      for (let i = 0; i < strings.length - 1; i++) {
        sink += compareUTF8(strings[i], strings[i + 1]);
      }
      if (sink === Infinity) throw new Error('noop');
    });

    bench(`compareJS (${label})`, () => {
      let sink = 0;
      for (let i = 0; i < strings.length - 1; i++) {
        sink += compareJS(strings[i], strings[i + 1]);
      }
      if (sink === Infinity) throw new Error('noop');
    });

    bench(`Intl.Collator (${label})`, () => {
      let sink = 0;
      for (let i = 0; i < strings.length - 1; i++) {
        sink += compareIntl(strings[i], strings[i + 1]);
      }
      if (sink === Infinity) throw new Error('noop');
    });

    bench(`localeCompare (${label})`, () => {
      let sink = 0;
      for (let i = 0; i < strings.length - 1; i++) {
        sink += strings[i].localeCompare(strings[i + 1]);
      }
      if (sink === Infinity) throw new Error('noop');
    });

    bench(`compareEnUS (${label})`, () => {
      let sink = 0;
      for (let i = 0; i < strings.length - 1; i++) {
        sink += compareEnUS(strings[i], strings[i + 1]);
      }
      if (sink === Infinity) throw new Error('noop');
    });

    bench(`compareSvSE (${label})`, () => {
      let sink = 0;
      for (let i = 0; i < strings.length - 1; i++) {
        sink += compareSvSE(strings[i], strings[i + 1]);
      }
      if (sink === Infinity) throw new Error('noop');
    });

    bench(`compareDeDE (${label})`, () => {
      let sink = 0;
      for (let i = 0; i < strings.length - 1; i++) {
        sink += compareDeDE(strings[i], strings[i + 1]);
      }
      if (sink === Infinity) throw new Error('noop');
    });

    bench(`compareEnGB (${label})`, () => {
      let sink = 0;
      for (let i = 0; i < strings.length - 1; i++) {
        sink += compareEnGB(strings[i], strings[i + 1]);
      }
      if (sink === Infinity) throw new Error('noop');
    });

    bench(`compareFrFR (${label})`, () => {
      let sink = 0;
      for (let i = 0; i < strings.length - 1; i++) {
        sink += compareFrFR(strings[i], strings[i + 1]);
      }
      if (sink === Infinity) throw new Error('noop');
    });
  });
}

await run();
