@TestOn('vm')
library;

// 10,000 mixed tasks with random crashes, timeouts and cancellations. After
// the storm every slot must be free, every worker back and idle, and every
// byte of native memory returned.

import 'dart:async';
import 'dart:math' as math;

import 'package:aether_thread/aether_thread.dart';
import 'package:flutter_test/flutter_test.dart';

import 'support.dart';

void main() {
  test(
    'soak: 10,000 tasks with crashes, timeouts and cancels leak nothing',
    () async {
      const total = 10000;
      const workers = 4;
      const slots = 6;
      final random = math.Random(20261011);
      final frames = ManualFrameScheduler();
      final memoryBefore = AetherMemoryStats.liveAllocations;
      await AetherConcurrentRunner.initialize(
        config: const AetherConfig(
          pool: AetherPoolConfig(
            workerCount: workers,
            taskTimeout: Duration(seconds: 5),
            watchdogTick: Duration(milliseconds: 5),
            agingInterval: Duration(milliseconds: 20),
            cleanupInterval: Duration(milliseconds: 100),
          ),
          slotCount: slots,
          slotStrideFloats: 64,
          maxPendingGrid: total,
          maxPendingFrameUpdates: 64,
        ),
        frameScheduler: frames,
      );

      final outcomes = <String, int>{};
      void count(String key) => outcomes[key] = (outcomes[key] ?? 0) + 1;

      final futures = <Future<void>>[];
      for (var i = 0; i < total; i++) {
        final roll = random.nextInt(1000);
        final priority =
            AetherPriority.values[random.nextInt(AetherPriority.values.length)];
        final token = AetherCancelToken();
        Future<Object?> work;
        if (roll < 10) {
          work = AetherConcurrentRunner.computeAsync(
            suicideTask(),
            priority: priority,
          );
        } else if (roll < 15) {
          work = AetherConcurrentRunner.computeAsync(
            spinForeverTask(),
            priority: priority,
            timeout: const Duration(milliseconds: 20),
          );
        } else if (roll < 35) {
          work = AetherConcurrentRunner.computeAsync(
            throwTask('soak $i'),
            priority: priority,
          );
        } else if (roll < 65) {
          work = AetherConcurrentRunner.computeAsync(
            sleepTask(2, i),
            priority: priority,
            cancelToken: token,
          );
          final hard = random.nextBool();
          Timer(
            Duration(microseconds: random.nextInt(3000)),
            () => token.cancel(hard: hard),
          );
        } else if (roll < 165) {
          work = AetherConcurrentRunner.computeGrid(
            kernel: fillKernel,
            nodeCount: 1 + random.nextInt(16),
            priority: priority,
            cancelToken: token,
          );
          if (random.nextInt(10) == 0) {
            Timer(Duration(microseconds: random.nextInt(2000)), token.cancel);
          }
        } else {
          work = AetherConcurrentRunner.computeAsync(
            addTask(i, 1),
            priority: priority,
          );
        }
        futures.add(
          work.then<void>(
            (_) => count('ok'),
            onError: (Object error) => count(error.runtimeType.toString()),
          ),
        );
      }

      // Pump frames until every task has settled.
      final all = Future.wait(futures);
      await pumpUntilDone(frames, all, timeout: const Duration(minutes: 4));

      final settled = outcomes.values.fold<int>(0, (a, b) => a + b);
      expect(settled, total);
      expect(outcomes['ok'], greaterThan(total ~/ 2));
      expect(outcomes['AetherWorkerCrash'], greaterThan(0));
      expect(outcomes['AetherTaskTimeout'], greaterThan(0));
      expect(outcomes['AetherTaskCancelled'], greaterThan(0));
      expect(outcomes['AetherTaskException'], greaterThan(0));

      // Every worker comes back after the crashes and kills.
      await waitUntil(
        () => AetherConcurrentRunner.stats().pool.idleWorkers == workers,
      );
      final stats = AetherConcurrentRunner.stats();
      expect(stats.freeSlots, slots);
      expect(stats.slotWaiters, 0);
      expect(stats.pool.queued, 0);
      expect(stats.pool.busyWorkers, 0);
      expect(
        stats.pool.completedTasks + stats.pool.failedTasks,
        lessThanOrEqualTo(total),
      );

      // ignore: avoid_print
      print('soak outcomes: $outcomes');
      // ignore: avoid_print
      print('soak stats: $stats');

      await AetherConcurrentRunner.dispose();
      expect(AetherMemoryStats.liveAllocations, memoryBefore);
    },
    timeout: const Timeout(Duration(minutes: 5)),
  );
}
