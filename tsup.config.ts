import { defineConfig } from 'tsup';

export default defineConfig({
  entry: ['src/index.ts'],
  format: ['cjs', 'esm'],
  dts: true,
  splitting: false,
  sourcemap: false,
  clean: true,
  treeshake: true,
  minify: true,
  outDir: 'dist',
  target: 'es2020',
  external: ['@aws-sdk/client-dynamodb', '@aws-sdk/lib-dynamodb'],
});
