@TestOn('vm')
library;

import 'dart:typed_data';

import 'package:aether_thread/aether_thread.dart';
import 'package:flutter_test/flutter_test.dart';

import 'support.dart';

const _pool = AetherPoolConfig(
  workerCount: 2,
  watchdogTick: Duration(milliseconds: 5),
  cleanupInterval: Duration(milliseconds: 200),
);

void main() {
  late ManualFrameScheduler frames;
  late List<AetherFailure> failures;

  Future<void> init({
    AetherConfig config = const AetherConfig(
      pool: _pool,
      slotCount: 4,
      slotStrideFloats: 256,
    ),
  }) async {
    frames = ManualFrameScheduler();
    failures = <AetherFailure>[];
    await AetherConcurrentRunner.initialize(
      config: config,
      onFailure: failures.add,
      frameScheduler: frames,
    );
  }

  tearDown(AetherConcurrentRunner.dispose);

  test('computeAsync returns without waiting for a frame', () async {
    await init();
    expect(await AetherConcurrentRunner.computeAsync(addTask(40, 2)), 42);
    expect(frames.frames, 0);
    expect(AetherConcurrentRunner.isInitialized, isTrue);
  });

  test('computeAsync before initialize is a clear StateError', () {
    expect(
      () => AetherConcurrentRunner.computeAsync(addTask(1, 1)),
      throwsStateError,
    );
  });

  test('calls made while initialize is running wait for it', () async {
    frames = ManualFrameScheduler();
    final starting = AetherConcurrentRunner.initialize(
      config: const AetherConfig(pool: _pool, slotCount: 2),
      frameScheduler: frames,
    );
    final result = AetherConcurrentRunner.computeAsync(addTask(1, 2));
    await starting;
    expect(await result, 3);
  });

  test('nested computeAsync on a worker runs inline', () async {
    await init();
    expect(await AetherConcurrentRunner.computeAsync(nestedTask()), 42);
  });

  test('same config initialize is a no-op; a different one throws', () async {
    await init();
    await AetherConcurrentRunner.initialize(
      config: const AetherConfig(
        pool: _pool,
        slotCount: 4,
        slotStrideFloats: 256,
      ),
      frameScheduler: frames,
    );
    await expectLater(
      AetherConcurrentRunner.initialize(
        config: const AetherConfig(pool: _pool, slotCount: 6),
        frameScheduler: frames,
      ),
      throwsStateError,
    );
  });

  test(
    'computeGrid prepares, computes in place and applies in a frame',
    () async {
      await init();
      Float32List? seen;
      final future = AetherConcurrentRunner.computeGrid(
        kernel: doubleKernel,
        nodeCount: 2,
        prepare: (slot, payload) {
          for (var i = 0; i < payload.floatCount; i++) {
            slot[i] = i.toDouble();
          }
        },
        onApplied: (slot, payload) =>
            seen = Float32List.fromList(slot.sublist(0, payload.floatCount)),
      );
      await waitUntil(() => frames.hasScheduledFrame);
      expect(seen, isNull, reason: 'nothing is applied before the frame');
      frames.pumpFrame();
      final payload = await future;
      expect(seen, <double>[0, 2, 4, 6, 8, 10, 12, 14]);
      expect(payload.nodeCount, 2);
      expect(AetherConcurrentRunner.stats().freeSlots, 4);
    },
  );

  test(
    'computeGrid waits for a slot, then fails past the wait limit',
    () async {
      await init(
        config: const AetherConfig(
          pool: _pool,
          slotCount: 1,
          slotStrideFloats: 16,
          maxPendingGrid: 1,
          maxPendingFrameUpdates: 8,
        ),
      );
      final first = AetherConcurrentRunner.computeGrid(
        kernel: slowKernel(60),
        nodeCount: 1,
      );
      final second = AetherConcurrentRunner.computeGrid(
        kernel: fillKernel,
        nodeCount: 1,
      );
      await expectLater(
        AetherConcurrentRunner.computeGrid(kernel: fillKernel, nodeCount: 1),
        throwsA(isA<AetherCapacityExceeded>()),
      );
      await pumpUntilDone(frames, first);
      await pumpUntilDone(frames, second);
      expect(AetherConcurrentRunner.stats().freeSlots, 1);
      expect(failures.whereType<AetherCapacityExceeded>(), hasLength(1));
    },
  );

  test('a cancelled slot waiter fails and leaves the queue', () async {
    await init(
      config: const AetherConfig(
        pool: _pool,
        slotCount: 1,
        slotStrideFloats: 16,
        maxPendingFrameUpdates: 8,
      ),
    );
    final first = AetherConcurrentRunner.computeGrid(
      kernel: slowKernel(60),
      nodeCount: 1,
    );
    final token = AetherCancelToken();
    final waiting = AetherConcurrentRunner.computeGrid(
      kernel: fillKernel,
      nodeCount: 1,
      cancelToken: token,
    );
    await Future<void>.delayed(Duration.zero);
    expect(AetherConcurrentRunner.stats().slotWaiters, 1);
    token.cancel();
    await expectLater(waiting, throwsA(isA<AetherTaskCancelled>()));
    expect(AetherConcurrentRunner.stats().slotWaiters, 0);
    await pumpUntilDone(frames, first);
  });

  test('a failing grid kernel releases its slot and reports once', () async {
    await init();
    await expectLater(
      AetherConcurrentRunner.computeGrid(
        kernel: (slot, payload) => throw StateError('kernel'),
        nodeCount: 1,
      ),
      throwsA(isA<AetherTaskException>()),
    );
    expect(AetherConcurrentRunner.stats().freeSlots, 4);
    final token = AetherCancelToken();
    final timedOut = AetherConcurrentRunner.computeGrid(
      kernel: slowKernel(400),
      nodeCount: 1,
      timeout: const Duration(milliseconds: 50),
      cancelToken: token,
    );
    await expectLater(timedOut, throwsA(isA<AetherTaskTimeout>()));
    expect(AetherConcurrentRunner.stats().freeSlots, 4);
    await waitUntil(() => frames.hasScheduledFrame);
    frames.pumpFrame();
    expect(failures.whereType<AetherTaskTimeout>(), hasLength(1));
    expect(failures.whereType<AetherTaskException>(), hasLength(1));
  });

  test('onFailure is delivered in a frame, not synchronously', () async {
    await init();
    await expectLater(
      AetherConcurrentRunner.computeAsync(throwTask('x')),
      throwsA(isA<AetherTaskException>()),
    );
    expect(failures, isEmpty);
    frames.pumpFrame();
    expect(failures.single, isA<AetherTaskException>());
  });

  test('continuous grid swaps buffers once per frame', () async {
    await init();
    final grid = await AetherConcurrentRunner.startContinuous(
      kernel: incrementStep,
      nodeCount: 2,
      seed: (slot, payload) => slot.fillRange(0, payload.floatCount, 10),
    );
    expect(grid.notifier.value[0], 10);
    for (var step = 1; step <= 5; step++) {
      await waitUntil(() => frames.hasScheduledFrame);
      frames.pumpFrame();
      expect(grid.steps, step);
      expect(grid.notifier.value.sublist(0, 8), everyElement(10 + step));
    }
    expect(AetherConcurrentRunner.stats().freeSlots, 2);
    final stopping = grid.stop();
    await pumpUntilDone(frames, stopping);
    expect(grid.notifier.isActive, isFalse);
    expect(AetherConcurrentRunner.stats().freeSlots, 4);
  });

  test('continuous grid pauses, resumes, and stops after 3 failures', () async {
    await init();
    final grid = await AetherConcurrentRunner.startContinuous(
      kernel: incrementStep,
      nodeCount: 1,
    );
    await waitUntil(() => frames.hasScheduledFrame);
    grid.pause();
    frames.pumpFrame();
    expect(grid.steps, 1);
    await Future<void>.delayed(const Duration(milliseconds: 30));
    expect(frames.hasScheduledFrame, isFalse);
    grid.resume();
    await waitUntil(() => frames.hasScheduledFrame);
    frames.pumpFrame();
    expect(grid.steps, 2);
    await pumpUntilDone(frames, grid.stop());

    final failing = await AetherConcurrentRunner.startContinuous(
      kernel: failingStep,
      nodeCount: 1,
    );
    await pumpUntilDone(frames, failing.done);
    expect(failing.lastFailure, isA<AetherTaskException>());
    expect(AetherConcurrentRunner.stats().freeSlots, 4);
  });

  test('dispose during in-flight work fails it and frees everything', () async {
    final before = AetherMemoryStats.liveAllocations;
    await init();
    expect(AetherMemoryStats.liveAllocations, before + 1);
    final running = AetherConcurrentRunner.computeAsync(spinForeverTask());
    final grid = AetherConcurrentRunner.computeGrid(
      kernel: slowKernel(300),
      nodeCount: 1,
    );
    final continuous = await AetherConcurrentRunner.startContinuous(
      kernel: incrementStep,
      nodeCount: 1,
    );
    final runningFails = expectLater(
      running,
      throwsA(isA<AetherPoolShutdown>()),
    );
    final gridFails = expectLater(grid, throwsA(isA<AetherPoolShutdown>()));
    await Future<void>.delayed(const Duration(milliseconds: 20));
    await AetherConcurrentRunner.dispose();
    await runningFails;
    await gridFails;
    await continuous.done;
    expect(AetherConcurrentRunner.isInitialized, isFalse);
    expect(AetherMemoryStats.liveAllocations, before);
    // And it can start again.
    await init();
    expect(await AetherConcurrentRunner.computeAsync(addTask(1, 1)), 2);
  });

  test('a failing workerInit leaves nothing allocated', () async {
    final before = AetherMemoryStats.liveAllocations;
    await expectLater(
      AetherConcurrentRunner.initialize(
        config: const AetherConfig(pool: _pool, slotCount: 2),
        workerInit: failingInit,
        frameScheduler: ManualFrameScheduler(),
      ),
      throwsA(isA<AetherWorkerInitFailure>()),
    );
    expect(AetherMemoryStats.liveAllocations, before);
    expect(AetherConcurrentRunner.isInitialized, isFalse);
  });

  test('rejects frame limits that slots could fill', () async {
    await expectLater(
      AetherConcurrentRunner.initialize(
        config: const AetherConfig(slotCount: 8, maxPendingFrameUpdates: 8),
        frameScheduler: ManualFrameScheduler(),
      ),
      throwsArgumentError,
    );
  });
}
