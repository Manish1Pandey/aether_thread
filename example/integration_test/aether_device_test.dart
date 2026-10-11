// On-device checks: run with
//   flutter test integration_test -d <device>
//
// Uses the real Flutter binding and real frames (no manual scheduler), real
// worker isolates and the platform's native allocator.

import 'dart:async';
import 'dart:io' show Platform, sleep;
import 'dart:isolate';
import 'dart:typed_data';

import 'package:aether_thread/aether_thread.dart';
import 'package:flutter/foundation.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:integration_test/integration_test.dart';

// ---- Task bodies (top-level, so they capture nothing) ----------------------

int Function() addTask(int a, int b) =>
    () => a + b;

int Function() spinTask() => () {
  var x = 0;
  while (true) {
    x = (x + 1) & 0xff;
  }
};

int Function() suicideTask() => () {
  Isolate.current.kill(priority: Isolate.immediate);
  sleep(const Duration(seconds: 5));
  return 0;
};

int Function() capturingTask(ReceivePort port) =>
    () => port.hashCode;

Future<int> Function() nestedTask() =>
    () => AetherConcurrentRunner.computeAsync(addTask(20, 22));

void doubleKernel(Float32List slot, AetherPayload payload) {
  for (var i = 0; i < payload.floatCount; i++) {
    slot[i] *= 2;
  }
}

void incrementStep(Float32List previous, Float32List next, AetherPayload p) {
  for (var i = 0; i < p.floatCount; i++) {
    next[i] = previous[i] + 1;
  }
}

int square(int x) => x * x;

AetherProgressTask<int> countTask(int steps) => (report) {
  for (var i = 1; i <= steps; i++) {
    sleep(const Duration(milliseconds: 2));
    report(i / steps);
  }
  return steps;
};

Float32List Function() bigTask(int floats) => () {
  final list = Float32List(floats);
  list[floats - 1] = 7;
  return list;
};

void simdKernel(Float32List slot, AetherPayload payload) {
  final lanes = slot.asFloat32x4();
  final two = Float32x4.splat(2);
  for (var i = 0; i < payload.floatCount ~/ 4; i++) {
    lanes[i] = lanes[i] * two;
  }
}

// ---- Helpers ---------------------------------------------------------------

/// Awaits [future] in real time, pumping real frames so frame-aligned work
/// can land.
Future<T> settle<T>(WidgetTester tester, Future<T> future) async {
  var done = false;
  Object? error;
  T? value;
  future.then<void>(
    (v) {
      value = v;
      done = true;
    },
    onError: (Object e) {
      error = e;
      done = true;
    },
  );
  final watch = Stopwatch()..start();
  while (!done) {
    if (watch.elapsed > const Duration(seconds: 30)) {
      throw TimeoutException('did not settle');
    }
    await tester.runAsync(
      () => Future<void>.delayed(const Duration(milliseconds: 4)),
    );
    await tester.pump();
  }
  if (error != null) {
    throw error!;
  }
  return value as T;
}

Future<Object?> failureOf(WidgetTester tester, Future<Object?> future) async {
  try {
    await settle(tester, future);
  } on Object catch (e) {
    return e;
  }
  return null;
}

void main() {
  IntegrationTestWidgetsFlutterBinding.ensureInitialized();

  testWidgets('Aether runs correctly on ${Platform.operatingSystem}', (
    tester,
  ) async {
    final memoryBefore = AetherMemoryStats.liveAllocations;
    final failures = <AetherFailure>[];
    await tester.runAsync(
      () => AetherConcurrentRunner.initialize(
        config: const AetherConfig(
          pool: AetherPoolConfig(watchdogTick: Duration(milliseconds: 5)),
          slotCount: 4,
          slotStrideFloats: 4096,
        ),
        onFailure: failures.add,
      ),
    );
    final stats0 = AetherConcurrentRunner.stats();
    debugPrint(
      'DEVICE ${Platform.operatingSystem}: ${stats0.pool.workerCount} workers, '
      '${Platform.numberOfProcessors} cores',
    );
    expect(stats0.pool.workerCount, greaterThanOrEqualTo(1));

    // computeAsync and nesting.
    expect(
      await settle(tester, AetherConcurrentRunner.computeAsync(addTask(2, 3))),
      5,
    );
    expect(
      await settle(tester, AetherConcurrentRunner.computeAsync(nestedTask())),
      42,
    );

    // Shared memory, applied in a real frame.
    Float32List? applied;
    final payload = await settle(
      tester,
      AetherConcurrentRunner.computeGrid(
        kernel: doubleKernel,
        nodeCount: 256,
        prepare: (slot, p) {
          for (var i = 0; i < p.floatCount; i++) {
            slot[i] = i.toDouble();
          }
        },
        onApplied: (slot, p) =>
            applied = Float32List.fromList(slot.sublist(0, p.floatCount)),
      ),
    );
    expect(payload.floatCount, 1024);
    expect(applied![1023], 2046);
    expect(applied![7], 14);

    // Continuous double-buffered grid over real frames.
    final grid = await tester.runAsync(
      () => AetherConcurrentRunner.startContinuous(
        kernel: incrementStep,
        nodeCount: 8,
      ),
    );
    final steps = Completer<void>();
    grid!.notifier.addListener(() {
      if (grid.steps >= 10 && !steps.isCompleted) {
        steps.complete();
      }
    });
    await settle(tester, steps.future);
    expect(grid.notifier.value[0], grid.steps.toDouble());
    await settle(tester, grid.stop());

    // Failure paths: timeout, crash, unsendable.
    expect(
      await failureOf(
        tester,
        AetherConcurrentRunner.computeAsync(
          spinTask(),
          timeout: const Duration(milliseconds: 100),
        ),
      ),
      isA<AetherTaskTimeout>(),
    );
    expect(
      await failureOf(
        tester,
        AetherConcurrentRunner.computeAsync(suicideTask()),
      ),
      isA<AetherWorkerCrash>(),
    );
    final port = ReceivePort();
    expect(
      await failureOf(
        tester,
        AetherConcurrentRunner.computeAsync(capturingTask(port)),
      ),
      isA<AetherUnsendableTask>(),
    );
    port.close();
    expect(
      await settle(tester, AetherConcurrentRunner.computeAsync(addTask(1, 1))),
      2,
    );
    await settle(
      tester,
      Future<void>.delayed(const Duration(milliseconds: 50)),
    );
    expect(failures.whereType<AetherTaskTimeout>(), hasLength(1));
    expect(failures.whereType<AetherWorkerCrash>(), hasLength(1));

    // New APIs: one-shot, parallel map, progress, SIMD, second runtime.
    final big = await settle(
      tester,
      AetherConcurrentRunner.computeOnce(bigTask(8 * 1024 * 1024)),
    );
    expect(big.last, 7);
    final squares = await settle(
      tester,
      AetherConcurrentRunner.computeMap(
        List<int>.generate(5000, (i) => i),
        square,
      ),
    );
    expect(squares[4999], 4999 * 4999);
    final progress = <double>[];
    expect(
      await settle(
        tester,
        AetherConcurrentRunner.computeWithProgress(
          countTask(40),
          onProgress: progress.add,
        ),
      ),
      40,
    );
    expect(progress.last, 1.0);
    Float32List? doubled;
    await settle(
      tester,
      AetherConcurrentRunner.computeGrid(
        kernel: simdKernel,
        nodeCount: 4,
        prepare: (slot, p) {
          for (var i = 0; i < p.floatCount; i++) {
            slot[i] = i.toDouble();
          }
        },
        onApplied: (slot, p) =>
            doubled = Float32List.fromList(slot.sublist(0, p.floatCount)),
      ),
    );
    expect(doubled![15], 30);
    final second = await tester.runAsync(
      () => AetherRuntime.start(
        config: const AetherConfig(
          pool: AetherPoolConfig(workerCount: 1),
          slotCount: 2,
          slotStrideFloats: 16,
        ),
      ),
    );
    expect(await settle(tester, second!.computeAsync(addTask(6, 7))), 13);
    await tester.runAsync(second.dispose);

    // A small latency sample vs Isolate.run (debug mode: for sanity only).
    final aether = <int>[];
    final plain = <int>[];
    for (var i = 0; i < 200; i++) {
      final sw = Stopwatch()..start();
      await tester.runAsync(
        () => AetherConcurrentRunner.computeAsync(addTask(i, 1)),
      );
      aether.add(sw.elapsedMicroseconds);
      sw
        ..reset()
        ..start();
      await tester.runAsync(() => Isolate.run(addTask(i, 1)));
      plain.add(sw.elapsedMicroseconds);
    }
    aether.sort();
    plain.sort();
    final stats = AetherConcurrentRunner.stats();
    debugPrint(
      'DEVICE ${Platform.operatingSystem}: small-task p50 Aether '
      '${aether[100]}us vs Isolate.run ${plain[100]}us (debug build); '
      'respawns ${stats.pool.respawns}, free slots ${stats.freeSlots}/4',
    );
    expect(stats.freeSlots, 4);

    await tester.runAsync(AetherConcurrentRunner.dispose);
    expect(AetherMemoryStats.liveAllocations, memoryBefore);
    debugPrint('DEVICE ${Platform.operatingSystem}: ALL CHECKS PASSED');
  });
}
