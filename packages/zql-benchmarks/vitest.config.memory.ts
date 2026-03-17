import {defineConfig} from 'vitest/config';

export default defineConfig({
  test: {
    name: 'memory',
    include: ['src/**/*.memory.bench.ts'],
    environment: 'node',
    browser: {
      enabled: false,
    },
  },
});
