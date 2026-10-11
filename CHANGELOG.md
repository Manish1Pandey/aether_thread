## 0.1.0

- Warm worker-isolate pool with priorities, aging, per-task timeouts, crash
  recovery and cancellation.
- Shared native memory grid with cache-line-aligned slots and zero-copy
  typed views (`asFloat32x4`, `asInt32`, `asUint32`, `asFloat64`, `asBytes`).
- Frame-aligned state stitcher.
- `AetherConcurrentRunner` / `AetherRuntime` API: `computeAsync`,
  `computeOnce`, `computeMap`, `computeWithProgress`, `computeGrid`,
  `startContinuous`, `stats`, `dispose`.
- `AetherSlotPainter` for drawing points straight from shared memory.
- Sealed `AetherFailure` hierarchy with actionable messages for unsendable
  closures and results.
- Web support: the same API runs on the web (JavaScript and WebAssembly)
  with tasks on the UI thread, selected automatically by conditional imports.
- `AetherMemoryStats` reports live slot memory on every platform.
