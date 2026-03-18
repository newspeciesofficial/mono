const FALLBACK_SENTINEL = 0xffffffff;
const BASE_SHIFT = 10; // diacritic(9b) + case(1b)
const DIAC_SHIFT = 1;

function buildDiacriticOrder(collator: Intl.Collator): string[] {
  const base = 'e';
  const cmp = (a: string, b: string): number =>
    collator.compare(base + a, base + b);
  const candidates: string[] = [];

  for (let cp = 0x0300; cp <= 0x036f; cp++) {
    const mark = String.fromCharCode(cp);
    const combined = base + mark;
    if (
      combined.normalize('NFC').normalize('NFD') === combined.normalize('NFD')
    ) {
      candidates.push(mark);
    }
  }

  candidates.sort(cmp);
  return ['', ...candidates];
}

function buildASCIIPrimaryRank(collator: Intl.Collator): Uint16Array {
  const baseCollator = new Intl.Collator(collator.resolvedOptions().locale, {
    sensitivity: 'base',
    usage: 'sort',
    numeric: collator.resolvedOptions().numeric,
    caseFirst: collator.resolvedOptions().caseFirst as "upper" | "lower" | "false",
    ignorePunctuation: collator.resolvedOptions().ignorePunctuation,
  });
  const chars: string[] = [];
  const seen = new Set<string>();

  for (let cp = 32; cp <= 126; cp++) {
    const canonical = String.fromCharCode(cp).toLowerCase();
    if (!seen.has(canonical)) {
      seen.add(canonical);
      chars.push(canonical);
    }
  }

  chars.sort((a, b) => baseCollator.compare(a, b));

  const rankTable = new Uint16Array(128);
  let rank = 1;
  for (let i = 0; i < chars.length; i++) {
    if (i > 0 && baseCollator.compare(chars[i - 1], chars[i]) !== 0) rank++;
    const code = chars[i].charCodeAt(0);
    rankTable[code] = rank;
    const upper = chars[i].toUpperCase().charCodeAt(0);
    if (upper < 128) rankTable[upper] = rank;
  }

  return rankTable;
}

function buildTable(
  collator: Intl.Collator,
  diacriticOrder: ReadonlyArray<string>,
): Uint32Array {
  const table = new Uint32Array(0x0250);
  const asciiPrimaryRank = buildASCIIPrimaryRank(collator);
  const diacMap = new Map<string, number>(
    diacriticOrder.map((m, i): [string, number] => [m, i]),
  );

  for (let cp = 0; cp < 0x0250; cp++) {
    const ch = String.fromCharCode(cp);
    const lower = ch.toLowerCase();
    const isUpper = lower !== ch ? 1 : 0;
    const canonical = isUpper ? lower : ch;

    const nfd = canonical.normalize('NFD');
    const stripped = nfd.replace(/[\u0300-\u036f]/g, '');

    if (stripped.length !== 1) {
      table[cp] = FALLBACK_SENTINEL;
      continue;
    }

    const baseCp = stripped.charCodeAt(0);
    const baseIdx =
      baseCp < asciiPrimaryRank.length ? asciiPrimaryRank[baseCp] : 0;
    if (baseIdx === 0) {
      table[cp] = FALLBACK_SENTINEL;
      continue;
    }

    const mark = nfd.slice(1);
    const diacIdx = diacMap.get(mark);
    if (diacIdx === undefined) {
      table[cp] = FALLBACK_SENTINEL;
      continue;
    }

    table[cp] = (baseIdx << BASE_SHIFT) | (diacIdx << DIAC_SHIFT) | isUpper;
  }

  return table;
}

export function createLocaleComparator(locale: string): (a: string, b: string) => number {
  const collator = new Intl.Collator(locale, {
    sensitivity: 'variant',
    usage: 'sort',
  });
  const table = buildTable(collator, buildDiacriticOrder(collator));
  const fallback = collator.compare.bind(collator);

  return (a: string, b: string): number => {
    const al = a.length;
    const bl = b.length;
    const minLen = al < bl ? al : bl;

    let firstDiacDiff = 0;
    let firstCaseDiff = 0;

    for (let i = 0; i < minLen; i++) {
      const acp = a.charCodeAt(i);
      const bcp = b.charCodeAt(i);

      if (acp === bcp) continue;

      if (acp >= 0x0250 || bcp >= 0x0250) {
        return fallback(a, b);
      }

      // | 0 sign-extends Uint32 values to signed integers, keeping them as
      // SMIs in V8. FALLBACK_SENTINEL (0xffffffff) becomes -1, so `< 0` is
      // the sentinel check. Legitimate packed weights never reach bit 31.
      const aw = table[acp] | 0;
      const bw = table[bcp] | 0;

      if (aw < 0 || bw < 0) {
        return fallback(a, b);
      }

      const aBase = aw >> BASE_SHIFT;
      const bBase = bw >> BASE_SHIFT;
      if (aBase !== bBase) return aBase - bBase;

      if (firstDiacDiff === 0) {
        const ad = (aw >> DIAC_SHIFT) & 0x1ff;
        const bd = (bw >> DIAC_SHIFT) & 0x1ff;
        if (ad !== bd) firstDiacDiff = ad - bd;
      }

      if (firstCaseDiff === 0) {
        const ac = aw & 1;
        const bc = bw & 1;
        if (ac !== bc) firstCaseDiff = ac - bc;
      }
    }

    if (al !== bl) {
      const longerStr = al > bl ? a : b;
      const nextCp = longerStr.charCodeAt(minLen);
      if (nextCp >= 0x0250) return fallback(a, b);
      const nw = table[nextCp] | 0;
      if (nw < 0) return fallback(a, b);
      return al > bl ? 1 : -1;
    }

    if (firstDiacDiff !== 0) return firstDiacDiff;
    if (firstCaseDiff !== 0) return firstCaseDiff;
    return 0;
  };
}
