# Aether Thread — Functional Specification

Version 0.1.0 · 2026-09-30

## 1. Purpose

Aether Thread lets a Flutter app move heavy computation off the UI thread without
the app ever touching `Isolate`, `SendPort` or `ReceivePort`. It keeps a pool of
worker isolates warm for the whole app lifetime, shares bulk numeric data with
them through native memory that is never copied, and applies finished results to
the UI at a fixed point in the frame so widgets never observe half-written state.

## 2. What it does

| # | Capability | Entry point |
|---|---|---|
| F1 | Run any sendable closure on a warm worker and get its result as a `Future`. | `AetherConcurrentRunner.computeAsync<T>()` |
| F2 | Run a kernel over a shared native `Float32List` slot in place (no copy in either direction) and apply the result in the next frame. | `AetherConcurrentRunner.computeGrid()` |
| F3 | Run a stepping kernel continuously, one step per frame, double-buffered: the worker writes the back slot while the UI reads the front slot. | `AetherConcurrentRunner.startContinuous()` |
| F4 | Paint a slot's points straight from native memory. | `AetherSlotPainter` |
| F5 | Report every failure as a typed `AetherFailure`, both on the task's `Future` and to a global `onFailure` callback, without crashing the app. | `AetherFailure` (sealed) |
| F6 | Cancel queued or running work. | `AetherCancelToken` |
| F7 | Expose live health and latency numbers, plus DevTools timeline events per task. | `AetherConcurrentRunner.stats()` |
| F8 | Run a set-up function once per worker (and again after every respawn). | `initialize(workerInit:)` |
| F9 | Run a one-off task on a fresh isolate and hand its result over without copying, with timeouts and cancellation. | `computeOnce()` |
| F10 | Apply a function to every list element across all workers, keeping order. | `computeMap()` |
| F11 | Report a running task's progress to the UI. | `computeWithProgress()` |
| F12 | Run several independently configured instances. | `AetherRuntime` |
| F13 | View slot memory as SIMD and other typed lists without copying. | `asFloat32x4()` and friends |
| F14 | Run the same API on the web, with tasks on the UI thread (no parallelism). | automatic, via conditional imports |

## 3. Functional requirements

### Memory grid (M1)
- **FR-M1** One native allocation holds every slot's header row (`Int64`) and float data. It is zero-filled on allocation.
- **FR-M2** Offsets are computed as `byteOffset = ((slot × slotStrideFloats) + (node × nodeStride) + field) × 4`, relative to the start of the float region. Every index is bounds-checked.
- **FR-M3** Node read and write copy into caller-supplied lists and allocate nothing. Slot views are created once at allocation time and reused.
- **FR-M4** Only the main isolate owns the slot free-list. Releasing a slot twice throws `StateError`.
- **FR-M5** Memory is freed by `dispose()`, or by a `NativeFinalizer` if `dispose()` never runs (hot restart, isolate group shutdown). Using a buffer after `dispose()` throws `StateError`.
- **FR-M6** `AetherPayload` (taskId, slotIndex, nodeCount, nodeStride, priority) packs into and unpacks from the slot's header row, so the worker reads the task header straight from shared memory.

### Worker pool (M2)
- **FR-P1** The default worker count is `max(1, numberOfProcessors − 2)`, leaving a core each for Flutter's UI and raster threads. It can be configured.
- **FR-P2** `initialize` completes only after every worker has run `workerInit` and completed its handshake. If any worker fails to start, initialisation is rolled back and fails with `AetherWorkerInitFailure`.
- **FR-P3** There are three FIFO queues: high, normal and low. An idle worker always takes the oldest task in the highest non-empty queue.
- **FR-P4** Aging: a task's effective priority is its submitted level plus one for every `agingInterval` it has waited, capped at high. Ties go to the task that has waited longest, so a low task outranks fresh high work after at most 2 × `agingInterval`.
- **FR-P5** A worker that exits unexpectedly fails its in-flight task with `AetherWorkerCrash` and is respawned. Repeated start-up failures back off exponentially (100 ms up to 5 s).
- **FR-P6** A task that runs past its timeout gets its worker killed (`Isolate.immediate`), then fails with `AetherTaskTimeout`, and the worker is respawned.
- **FR-P7** A grid task's `Future` completes (success or failure) only after its worker has stopped touching the slot: either it replied, or its exit has been confirmed. Slots are released only then.
- **FR-P8** Watchdog clocks, and `computeOnce` timeouts, pause while the app is paused or hidden and resume on `resumed`.
- **FR-P9** A closure that cannot be sent fails immediately with `AetherUnsendableTask`, and the worker is not affected. A result that cannot be sent back fails with `AetherUnsendableResult`.
- **FR-P10** A periodic cleanup pass removes cancelled entries from the queues and respawns any worker left dead.
- **FR-P11** Shutdown runs in this order: stop accepting → fail queued (`AetherPoolShutdown`) → kill workers → wait for every exit → fail in-flight tasks → close ports.

### State stitcher (M3)
- **FR-S1** Completed grid results are applied in a transient frame callback (`scheduleFrameCallback`), before build, so widgets see the new data in the same frame.
- **FR-S2** Every update received between two frames is applied in a single batch. Updates for the same slot are coalesced, keeping the newest.
- **FR-S3** Slots are recycled in a post-frame callback, after the frame has read them.
- **FR-S7** While frames are off (app hidden or paused), grid results and failure notices are applied and their slots recycled straight away, outside a frame, so they are never held until the app returns. Pending updates are flushed when the app is hidden or paused, and a frame callback registered before that becomes a no-op.
- **FR-S8** Continuous grids stay frame-paced: while frames are off they submit no new steps, and resume on the first frame after the app returns.
- **FR-S4** The pending list is bounded. When the unkeyed (failure-notice) backlog is full, the oldest notice is dropped and counted.
- **FR-S5** `computeAsync` futures never wait for a frame. Only grid and continuous updates do.
- **FR-S6** An exception thrown by a user `onApplied` callback is reported through `FlutterError.reportError` and does not stop the rest of the batch.

### Web backend (W)
- **FR-W1** On platforms with `dart.library.js_interop`, the memory grid, pool and one-shot runner are replaced at compile time by web implementations with the same public API. No `dart:ffi`, `dart:io` or `dart:isolate` import reaches a web build.
- **FR-W2** Web slots are heap memory: `Float32List` data, and `List<int>` header rows (`Int64List` does not exist in JavaScript). `ContiguousMemoryBuffer.attach` throws `UnsupportedError`.
- **FR-W3** The web pool runs one task at a time on the UI thread, one per event-loop turn (`Timer.run`), with the same priority queues and aging rule. It reports `workerCount` as 1.
- **FR-W4** Tasks run in a zone carrying a marker, so nested runner calls run inline across `await`s. The marker is switched off when the task (or `workerInit`) ends, so timers and listeners it created count as ordinary app code afterwards.
- **FR-W5** A timeout or hard cancel abandons a running value task: its future fails immediately, the queue moves on, and its eventual result is discarded. Grid and step kernels write a slot, so their timeout or cancel is delivered only when the kernel finishes, and the queue waits for it: a slot is never handed on while it is still being written. `AetherWorkerCrash` and the unsendable failures do not occur on the web.

### Runner (M4)
- **FR-R1** Calling `initialize` again with the same configuration does nothing. A different configuration throws `StateError` until `dispose()` has been called.
- **FR-R2** When already running on a worker, `computeAsync` runs the task inline instead of queueing it, so nested calls cannot deadlock the pool. `computeGrid` and `startContinuous` throw `StateError` there, because slots are owned by the main isolate.
- **FR-R3** When no slot is free, `computeGrid` waits in a queue of at most `maxPendingGrid` requests. Beyond that it fails with `AetherCapacityExceeded`.
- **FR-R4** On `AppLifecycleState.detached` the runner disposes itself. On `paused` or `hidden` the workers stay warm and only the watchdog is paused.
- **FR-R5** Cancelling a queued task fails it right away with `AetherTaskCancelled`. Cancelling a running task discards its result when it finishes. `hard: true` kills the worker and respawns it.
- **FR-R6** A continuous grid stops by itself after 3 consecutive failed steps.
- **FR-R7** `computeOnce` returns its result through `Isolate.exit` (no copy). Its isolate is killed on timeout, cancellation or dispose, with the matching failure.
- **FR-R8** `computeMap` splits the list into chunks (default: about four per worker), returns results in input order, and on the first failure cancels the remaining chunks and rethrows that failure.
- **FR-R9** Progress reports are clamped to 0.0–1.0 and throttled to one per 16 ms on the worker; 1.0 always gets through. Reports from a cancelled task are dropped.
- **FR-R10** Unsendable-closure and unsendable-result failures name the offending class and library and say how to fix it.
- **FR-R11** Each `AetherRuntime` owns its own grid, pool, one-shot runner and stitcher; disposing one does not affect another. `AetherConcurrentRunner` is a static front for one default runtime.

## 4. Data flow

```
 main isolate                                            worker isolate k
 ────────────                                            ────────────────
 computeGrid(kernel)
   │ acquireSlot()  (free-list, main only)
   │ prepare(slotView)          ── writes native memory ─┐
   │ payload.packInto(header[slot])                      │   shared native block
   ▼                                                     │   [headers | slot0 | slot1 | …]
 AetherWorkerPool.submit ─ SendPort(taskId, slot, fn) ──►│◄── worker reads header[slot]
   │   priority queues + aging                           │    kernel(slotView)   (in place)
   │   watchdog (per-worker Stopwatch)                   │
   │◄──────────── SendPort(taskId, status) ──────────────┘
   ▼
 AetherStateStitcher.enqueue(update keyed by slot)
   │   scheduleFrameCallback ──► [frame N, before build] apply → onApplied / notifier swap
   │   addPostFrameCallback  ──► [frame N, after paint]  releaseSlot / submit next step
   ▼
 Future completes · onFailure(AetherFailure) for failures
```

Ownership rule: exactly one isolate may write a slot at any moment. Ownership passes
from main to worker when the task message is sent, and back when the reply arrives
or the worker's exit is confirmed. Dart's message delivery order makes the writes
before a send visible to the receiver, so no atomics are needed.

## 5. Can / cannot

| Asked for | Built | Reason |
|---|---|---|
| Share a `Float32List` across threads | A native-backed `Float32List` (`Pointer<Float>.asTypedList`) re-attached in every worker. Genuinely shared, never copied. | A heap `Float32List` cannot be shared between isolates. |
| Zero dependencies | Flutter SDK only. `malloc`/`free` bound directly through `dart:ffi` (`CoTaskMemAlloc` on Windows). | — |
| No reflection or codegen | None used. | — |
| ZERO allocation on the hot path | **Not possible literally.** Every port message, `Future` and `Completer` allocates. Bulk data is never allocated or copied, and each task costs a small fixed number of objects regardless of data size. | Dart VM. |
| Sub-ms latency, locked 120/144 Hz | **Not guaranteed.** Dispatch-to-result is measured by the benchmark. UI-thread work per frame is proportional to the number of finished tasks, not their size. Grid results additionally wait for the next frame. | OS scheduler and device. |
| Kill frozen threads | `Isolate.kill(immediate)` plus respawn. It cannot interrupt a worker blocked inside a native FFI call until that call returns. | Dart VM. |
| Offload anonymous closures | Yes, if everything they capture is sendable. Otherwise `AetherUnsendableTask`. | Isolate-group messaging rules. |
| Large typed results via `TransferableTypedData` | **Rejected after measurement.** A 64 MB result stalled the main event loop 7–20 ms with `TransferableTypedData`, versus about 2.3 ms with a plain send (AOT, Apple Silicon). Plain send is used. For zero copies, use `computeGrid`. | Measured 2026-09-30. |
| Zero-copy to the GPU | No copy on the Dart side. `drawRawPoints` reads the native list directly, but the engine still records the points into its display list (one engine-side copy). | Flutter engine. |
| Parallel work on the web | **Not possible.** The web backend keeps the API and runs tasks on the UI thread; there is no speed-up there. Measured in the example app in Chrome: 25.5 fps on the "worker" path vs 26.7 fps on the UI thread at 100k particles. | No threads that can run Dart closures on the web. |
| Beat `compute()` everywhere | **Ties on one-off huge results.** A warm worker must copy its reply (5.6 ms for 32 MB), so `computeOnce` uses `Isolate.exit` like `compute()` does: 1.75–2.0 ms, a tie. Aether wins on small tasks (7×), bursts (2.5×), in-place kernels (1.8×, or 7× with SIMD) and parallel maps (6.5×). | Measured 2026-10-11, AOT, Apple Silicon. |
| No main-thread stalls under full load | **Not guaranteed.** With every worker saturated, the main thread stalled 1.3–10.5 ms in tests (CPU contention). The default worker count leaves two cores free to reduce this. | Measured 2026-10-11. |
