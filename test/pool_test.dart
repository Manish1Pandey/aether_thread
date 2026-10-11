@TestOn('vm')
library;

import 'dart:isolate';

import 'package:aether_thread/aether_thread.dart';
import 'package:flutter_test/flutter_test.dart';

import 'support.dart';

AetherPoolConfig _config({
  int workers = 2,
  Duration? timeout = const Duration(seconds: 10),
  Duration aging = const Duration(seconds: 10),
}) => AetherPoolConfig(
  workerCount: workers,
  taskTimeout: timeout,
  agingInterval: aging,
  watchdogTick: const Duration(milliseconds: 5),
  cleanupInterval: const Duration(milliseconds: 200),
);

void main() {
  late AetherWorkerPool pool;
  final failures = <AetherFailure>[];

  Future<AetherWorkerPool> start({
    int workers = 2,
    Duration? timeout = const Duration(seconds: 10),
    Duration aging = const Duration(seconds: 10),
    ContiguousMemoryBuffer? buffer,
    AetherWorkerInit? init,
  }) async {
    failures.clear();
    pool = await AetherWorkerPool.start(
      config: _config(workers: workers, timeout: timeout, aging: aging),
      buffer: buffer,
      workerInit: init,
    );
    pool.onFailure = failures.add;
    return pool;
  }

  tearDown(() async {
    await pool.shutdown();
  });

  test('runs a closure on a worker and returns the result', () async {
    await start();
    expect(await pool.run(addTask(2, 3)), 5);
    expect(await pool.run(isWorkerTask()), isTrue);
    expect(AetherWorkerPool.isWorkerIsolate, isFalse);
    final stats = pool.stats();
    expect(stats.completedTasks, 2);
    expect(stats.idleWorkers, 2);
    expect(stats.latencySamples, 2);
  });

  test('runs workerInit on every worker before work starts', () async {
    await start(init: markingInit);
    final seen = await Future.wait(<Future<bool>>[
      for (var i = 0; i < 6; i++) pool.run(initRanTask()),
    ]);
    expect(seen, everyElement(isTrue));
  });

  test('fails start-up cleanly when workerInit throws', () async {
    await expectLater(
      AetherWorkerPool.start(config: _config(), workerInit: failingInit),
      throwsA(isA<AetherWorkerInitFailure>()),
    );
    pool = await AetherWorkerPool.start(config: _config(workers: 1));
  });

  test('runs higher priority first', () async {
    await start(workers: 1);
    final order = <String>[];
    final blocker = pool.run(sleepTask(200, 0));
    await Future<void>.delayed(const Duration(milliseconds: 20));
    final futures = <Future<void>>[
      pool
          .run(addTask(0, 0), priority: AetherPriority.low)
          .then((_) => order.add('low')),
      pool.run(addTask(0, 0)).then((_) => order.add('normal')),
      pool
          .run(addTask(0, 0), priority: AetherPriority.high)
          .then((_) => order.add('high')),
    ];
    await blocker;
    await Future.wait(futures);
    expect(order, <String>['high', 'normal', 'low']);
  });

  test('aging lets an old low task beat fresh high work', () async {
    await start(workers: 1, aging: const Duration(milliseconds: 40));
    final order = <String>[];
    final blocker = pool.run(sleepTask(250, 0));
    await Future<void>.delayed(const Duration(milliseconds: 10));
    final low = pool
        .run(addTask(0, 0), priority: AetherPriority.low)
        .then((_) => order.add('low'));
    // The low task has waited ~200 ms (5 intervals) by the time these are
    // queued, so it already ranks as high and is older than both.
    await Future<void>.delayed(const Duration(milliseconds: 200));
    final highs = <Future<void>>[
      for (var i = 0; i < 2; i++)
        pool
            .run(addTask(0, 0), priority: AetherPriority.high)
            .then((_) => order.add('high')),
    ];
    await blocker;
    await Future.wait(<Future<void>>[low, ...highs]);
    expect(order.first, 'low');
  });

  test(
    'a throwing task fails with AetherTaskException and keeps the worker',
    () async {
      await start(workers: 1);
      await expectLater(
        pool.run(throwTask('nope')),
        throwsA(
          isA<AetherTaskException>()
              .having((f) => '${f.error}', 'error', contains('nope'))
              .having(
                (f) => f.remoteStackTrace.toString(),
                'stack',
                isNotEmpty,
              ),
        ),
      );
      expect(await pool.run(addTask(1, 1)), 2);
      expect(pool.stats().respawns, 0);
      expect(failures.single, isA<AetherTaskException>());
    },
  );

  test('a worker that kills itself is reported and respawned', () async {
    await start(workers: 1);
    await expectLater(
      pool.run(suicideTask()),
      throwsA(isA<AetherWorkerCrash>()),
    );
    expect(await pool.run(addTask(4, 4)), 8);
    expect(pool.stats().respawns, 1);
  });

  test(
    'an uncaught async error on a worker is a crash with its message',
    () async {
      await start(workers: 1);
      await expectLater(
        pool.run(uncaughtAsyncErrorTask()),
        throwsA(
          isA<AetherWorkerCrash>().having(
            (f) => f.message,
            'message',
            contains('uncaught on worker'),
          ),
        ),
      );
      expect(await pool.run(addTask(1, 2)), 3);
    },
  );

  test('a task past its timeout is killed and the worker respawned', () async {
    await start(workers: 1);
    final watch = Stopwatch()..start();
    await expectLater(
      pool.run(spinForeverTask(), timeout: const Duration(milliseconds: 80)),
      throwsA(isA<AetherTaskTimeout>()),
    );
    expect(watch.elapsedMilliseconds, lessThan(2000));
    expect(await pool.run(addTask(5, 5)), 10);
    expect(pool.stats().respawns, 1);
    expect(failures.single, isA<AetherTaskTimeout>());
  });

  test('a paused watchdog does not time out tasks', () async {
    await start(workers: 1);
    pool.pauseWatchdog();
    final result = pool.run(
      sleepTask(150, 7),
      timeout: const Duration(milliseconds: 30),
    );
    expect(await result, 7);
    pool.resumeWatchdog();
    expect(pool.isWatchdogPaused, isFalse);
  });

  test(
    'an unsendable closure fails fast and leaves the worker healthy',
    () async {
      await start(workers: 1);
      final port = ReceivePort();
      await expectLater(
        pool.run(capturingTask(port)),
        throwsA(isA<AetherUnsendableTask>()),
      );
      port.close();
      expect(await pool.run(addTask(1, 1)), 2);
      expect(pool.stats().respawns, 0);
    },
  );

  test('an unsendable result is reported, not hung', () async {
    await start(workers: 1);
    await expectLater(
      pool.run(unsendableResultTask()),
      throwsA(isA<AetherUnsendableResult>()),
    );
    expect(await pool.run(addTask(1, 1)), 2);
  });

  test('cancelling a queued task fails it at once', () async {
    await start(workers: 1);
    final blocker = pool.run(sleepTask(150, 0));
    final token = AetherCancelToken();
    final queued = pool.run(addTask(1, 1), cancelToken: token);
    token.cancel();
    await expectLater(queued, throwsA(isA<AetherTaskCancelled>()));
    await blocker;
    expect(pool.stats().queued, 0);
    expect(failures, isEmpty);
  });

  test('soft cancel of a running task discards its result', () async {
    await start(workers: 1);
    final token = AetherCancelToken();
    final running = pool.run(sleepTask(100, 1), cancelToken: token);
    await Future<void>.delayed(const Duration(milliseconds: 30));
    token.cancel();
    await expectLater(running, throwsA(isA<AetherTaskCancelled>()));
    expect(pool.stats().respawns, 0);
  });

  test('hard cancel kills the running worker and respawns it', () async {
    await start(workers: 1);
    final token = AetherCancelToken();
    final running = pool.run(spinForeverTask(), cancelToken: token);
    await Future<void>.delayed(const Duration(milliseconds: 30));
    token.cancel(hard: true);
    await expectLater(running, throwsA(isA<AetherTaskCancelled>()));
    expect(await pool.run(addTask(2, 2)), 4);
    expect(pool.stats().respawns, 1);
  });

  test('an already-cancelled token fails immediately', () async {
    await start(workers: 1);
    final token = AetherCancelToken()..cancel();
    await expectLater(
      pool.run(addTask(1, 1), cancelToken: token),
      throwsA(isA<AetherTaskCancelled>()),
    );
  });

  test('grid kernels change shared memory in place', () async {
    final buffer = ContiguousMemoryBuffer.allocate(
      slotCount: 2,
      slotStrideFloats: 64,
      nodeStride: 4,
    );
    addTearDown(buffer.dispose);
    await start(buffer: buffer);
    final slot = buffer.acquireSlot();
    buffer.writeHeader(
      AetherPayload(taskId: 1, slotIndex: slot, nodeCount: 3, nodeStride: 4),
    );
    await pool.runGrid(slot, fillKernel);
    expect(buffer.readField(slot, 2, 3), 23);
    await pool.runGrid(slot, doubleKernel);
    expect(buffer.readField(slot, 2, 3), 46);
    // Clean shutdown before the buffer is freed.
    await pool.shutdown();
  });

  test('grid tasks fail without a buffer', () async {
    await start();
    await expectLater(pool.runGrid(0, fillKernel), throwsStateError);
  });

  test(
    'shutdown fails queued and running work, then refuses new work',
    () async {
      await start(workers: 1);
      final running = pool.run(spinForeverTask());
      final queued = pool.run(addTask(1, 1));
      await Future<void>.delayed(const Duration(milliseconds: 30));
      final done = pool.shutdown();
      await expectLater(queued, throwsA(isA<AetherPoolShutdown>()));
      await expectLater(running, throwsA(isA<AetherPoolShutdown>()));
      await done;
      await expectLater(
        pool.run(addTask(1, 1)),
        throwsA(isA<AetherPoolShutdown>()),
      );
      expect(failures, isEmpty);
    },
  );

  test('runs many tasks in parallel across workers', () async {
    await start(workers: 4);
    final results = await Future.wait(<Future<int>>[
      for (var i = 0; i < 200; i++) pool.run(addTask(i, i)),
    ]);
    expect(results, <int>[for (var i = 0; i < 200; i++) 2 * i]);
    final stats = pool.stats();
    expect(stats.completedTasks, 200);
    expect(stats.p99Micros, greaterThanOrEqualTo(stats.p50Micros));
  });
}
