import { defineConfig } from "vitest/config";

// The correctness suites (test/correctness/*) share one live TimeStar server
// and some of them force memstore->TSM flushes that would move OTHER files'
// "memory-store only" datasets if files ran concurrently. Run files serially
// so data-placement expectations stay deterministic.
export default defineConfig({
  test: {
    fileParallelism: false,
    testTimeout: 30_000,
    hookTimeout: 180_000,
  },
});
