import {bootstrap} from '../helpers/runner.ts';
import {fuzzHydrationTests} from '../helpers/fuzz-hydration.ts';
import {getPagila} from './get-deps.ts';
import {schema} from './schema.ts';

const pgContent = await getPagila();

// Set this to reproduce a specific failure.
const REPRO_SEED = undefined;

const harness = await bootstrap({
  suiteName: 'pagila_fuzz_hydration',
  zqlSchema: schema,
  pgContent,
});

fuzzHydrationTests(schema, harness, REPRO_SEED);
