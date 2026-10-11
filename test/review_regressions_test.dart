@TestOn('vm')
library;

// Regression tests for the findings of the independent review.

import 'dart:async';
import 'dart:io';

import 'package:aether_thread/aether_thread.dart';
import 'package:flutter/foundation.dart';
import 'package:flutter_test/flutter_test.dart';

import 'support.dart';

const _pool = AetherPoolConfig(
  workerCount: 2,
  watchdogTick: Duration(milliseconds: 5),
  cleanupInterval: Duration(milliseconds: 100),
);

void main() {
  late ManualFrameScheduler frames;

  Future<void> init({int slots = 4}) async {
    frames = ManualFrameScheduler();
    await AetherConcurrentRunner.initialize(
      config: AetherConfig(pool: _pool, slotCount: slots, slotStrideFloats: 64),
      frameScheduler: frames,
    );
  }

  tearDown(AetherConcurrentRunner.dispose);

  test(
    '#1 concurrent continuous grids reserve slot pairs, no deadlock',
    () async {
      await init();
      final starts = <Future<AetherContinuousGrid>>[
        for (var i = 0; i < 3; i++)
          AetherConcurrentRunner.startContinuous(
            kernel: incrementStep,
            nodeCount: 1,
          ),
      ];
      final started = <AetherContinuousGrid>[];
      for (final start in starts.take(2)) {
        started.add(await start);
      }
      // Two grids hold all four slots; the third waits for a whole pair.
      expect(AetherConcurrentRunner.stats().freeSlots, 0);
      expect(AetherConcurrentRunner.stats().slotWaiters, 1);
      await pumpUntilDone(frames, started.first.stop());
      final third = await pumpUntilDone(frames, starts[2]);
      expect(third.isRunning, isTrue);
      await pumpUntilDone(frames, third.stop());
      await pumpUntilDone(frames, started[1].stop());
      expect(AetherConcurrentRunner.stats().freeSlots, 4);
    },
  );

  test('#1 a pair request is not starved by later single requests', () async {
    await init(slots: 2);
    final first = AetherConcurrentRunner.computeGrid(
      kernel: slowKernel(80),
      nodeCount: 1,
    );
    final pair = AetherConcurrentRunner.startContinuous(
      kernel: incrementStep,
      nodeCount: 1,
    );
    final single = AetherConcurrentRunner.computeGrid(
      kernel: fillKernel,
      nodeCount: 1,
    );
    await pumpUntilDone(frames, first);
    final grid = await pumpUntilDone(frames, pair);
    expect(
      AetherConcurrentRunner.stats().slotWaiters,
      1,
      reason: 'single waits',
    );
    await pumpUntilDone(frames, grid.stop());
    await pumpUntilDone(frames, single);
  });

  test('#1 startContinuous needs two slots', () async {
    await init(slots: 1);
    await expectLater(
      AetherConcurrentRunner.startContinuous(
        kernel: incrementStep,
        nodeCount: 1,
      ),
      throwsStateError,
    );
  });

  test(
    '#2 queued work fails instead of hanging when no worker can start',
    () async {
      final flag = File(
        '${Directory.systemTemp.path}/aether_init_${DateTime.now().microsecondsSinceEpoch}',
      );
      addTearDown(() {
        if (flag.existsSync()) {
          flag.deleteSync();
        }
      });
      final pool = await AetherWorkerPool.start(
        config: const AetherPoolConfig(
          workerCount: 1,
          watchdogTick: Duration(milliseconds: 5),
        ),
        workerInit: failIfFileExists(flag.path),
      );
      addTearDown(pool.shutdown);
      flag.writeAsStringSync('broken');
      await expectLater(
        pool.run(suicideTask()),
        throwsA(isA<AetherWorkerCrash>()),
      );
      final waiting = pool.run(
        addTask(1, 1),
        timeout: const Duration(milliseconds: 200),
      );
      await expectLater(
        waiting.timeout(const Duration(seconds: 10)),
        throwsA(isA<AetherWorkerInitFailure>()),
      );
      await expectLater(
        pool.run(addTask(1, 1)),
        throwsA(isA<AetherWorkerInitFailure>()),
      );
      // Recovers once start-up works again.
      flag.deleteSync();
      await waitUntil(() => pool.stats().idleWorkers == 1);
      expect(await pool.run(addTask(2, 2)), 4);
    },
  );

  test('#3 progress tasks run on a pool without a memory buffer', () async {
    final pool = await AetherWorkerPool.start(
      config: const AetherPoolConfig(workerCount: 1),
    );
    addTearDown(pool.shutdown);
    final seen = <double>[];
    expect(
      await pool.runWithProgress(progressTask(3, 1), onProgress: seen.add),
      3,
    );
    expect(seen.last, 1.0);
  });

  test('#5 grid results land without frames while frames are off', () async {
    await init();
    frames.framesEnabled = false;
    Float32List? seen;
    final payload = await AetherConcurrentRunner.computeGrid(
      kernel: fillKernel,
      nodeCount: 1,
      onApplied: (slot, p) => seen = Float32List.fromList(slot.sublist(0, 4)),
    ).timeout(const Duration(seconds: 5));
    expect(payload.nodeCount, 1);
    expect(seen, <double>[0, 1, 2, 3]);
    expect(frames.frames, 0);
    expect(AetherConcurrentRunner.stats().freeSlots, 4);
  });

  test('#5 flushWithoutFrame applies once; the stale frame does nothing', () {
    final scheduler = ManualFrameScheduler();
    final stitcher = AetherStateStitcher(
      keySpace: 2,
      maxPending: 4,
      scheduler: scheduler,
    );
    var applied = 0;
    var recycled = 0;
    stitcher.enqueue(
      AetherCallbackUpdate(
        key: 0,
        onApply: () => applied++,
        onAfterFrame: () => recycled++,
      ),
    );
    expect(scheduler.hasScheduledFrame, isTrue);
    stitcher.flushWithoutFrame();
    expect(applied, 1);
    expect(recycled, 1);
    scheduler.pumpFrame();
    expect(applied, 1);
    expect(recycled, 1);
  });

  test('#6 an onApplied error is reported and fails the future', () async {
    await init();
    final reported = <FlutterErrorDetails>[];
    final previous = FlutterError.onError;
    FlutterError.onError = reported.add;
    addTearDown(() => FlutterError.onError = previous);
    await expectLater(
      pumpUntilDone(
        frames,
        AetherConcurrentRunner.computeGrid(
          kernel: fillKernel,
          nodeCount: 1,
          onApplied: (slot, payload) => throw StateError('ui bug'),
        ),
      ),
      throwsStateError,
    );
    expect(reported.single.exception, isA<StateError>());
    expect(AetherConcurrentRunner.stats().freeSlots, 4);
  });

  test('#7 a throwing onFailure does not stop worker recovery', () async {
    final escaped = <Object>[];
    await runZonedGuarded(() async {
      final pool = await AetherWorkerPool.start(
        config: const AetherPoolConfig(workerCount: 1),
      );
      pool.onFailure = (failure) => throw StateError('listener bug');
      await expectLater(
        pool.run(suicideTask()),
        throwsA(isA<AetherWorkerCrash>()),
      );
      expect(
        await pool.run(addTask(3, 3)).timeout(const Duration(seconds: 2)),
        6,
      );
      await pool.shutdown();
    }, (error, stack) => escaped.add(error));
    expect(escaped.whereType<StateError>(), isNotEmpty);
  });

  test('#9 initialize fails if dispose runs during it', () async {
    final starting = AetherConcurrentRunner.initialize(
      config: const AetherConfig(pool: _pool, slotCount: 2),
      frameScheduler: ManualFrameScheduler(),
    );
    final disposing = AetherConcurrentRunner.dispose();
    await expectLater(starting, throwsStateError);
    await disposing;
    expect(AetherConcurrentRunner.isInitialized, isFalse);
  });

  test('#11 the shared block is aligned to a cache line', () {
    for (var i = 0; i < 20; i++) {
      final buffer = ContiguousMemoryBuffer.allocate(
        slotCount: 1 + i % 3,
        slotStrideFloats: 16 + i,
        nodeStride: 1,
      );
      expect(buffer.descriptor.address % 64, 0);
      expect(buffer.addressOf(buffer.slotCount - 1, 0, 0) % 64, 0);
      buffer.dispose();
    }
  });

  group('verification pass', () {
    test('V1 continuous grids pause while frames are off', () async {
      await init();
      final grid = await AetherConcurrentRunner.startContinuous(
        kernel: incrementStep,
        nodeCount: 1,
      );
      await waitUntil(() => frames.hasScheduledFrame);
      frames.pumpFrame();
      expect(grid.steps, 1);
      frames.framesEnabled = false;
      await Future<void>.delayed(const Duration(milliseconds: 200));
      // At most the step already in flight lands; nothing spins.
      expect(grid.steps, lessThanOrEqualTo(2));
      final paused = grid.steps;
      frames.framesEnabled = true;
      for (var i = 0; i < 3; i++) {
        await waitUntil(() => frames.hasScheduledFrame);
        frames.pumpFrame();
      }
      expect(grid.steps, greaterThan(paused));
      await pumpUntilDone(frames, grid.stop());
    });

    test('V2 one-shot timeouts pause in the background', () async {
      final runner = AetherOneShotRunner();
      final result = runner.run(
        sleepTask(200, 9),
        taskId: 1,
        timeout: const Duration(milliseconds: 60),
      );
      runner.pauseTimeouts();
      expect(await result, 9);
      runner.resumeTimeouts();
      await expectLater(
        runner.run(
          spinForeverTask(),
          taskId: 2,
          timeout: const Duration(milliseconds: 60),
        ),
        throwsA(isA<AetherTaskTimeout>()),
      );
    });

    test('V3 one-shot crashes keep their error details', () async {
      final runner = AetherOneShotRunner();
      await expectLater(
        runner.run(uncaughtAsyncErrorTask(), taskId: 3),
        throwsA(
          isA<AetherWorkerCrash>().having(
            (f) => f.message,
            'message',
            contains('uncaught on worker'),
          ),
        ),
      );
    });

    test('V4 nested calls inside computeOnce run inline', () async {
      await init();
      expect(await AetherConcurrentRunner.computeOnce(nestedTask()), 42);
    });

    test('V6 same-config initialize fails if dispose runs during it', () async {
      const config = AetherConfig(pool: _pool, slotCount: 2);
      final scheduler = ManualFrameScheduler();
      final first = AetherConcurrentRunner.initialize(
        config: config,
        frameScheduler: scheduler,
      );
      final second = AetherConcurrentRunner.initialize(
        config: config,
        frameScheduler: scheduler,
      );
      final disposing = AetherConcurrentRunner.dispose();
      await expectLater(first, throwsStateError);
      await expectLater(second, throwsStateError);
      await disposing;
    });
  });
}
