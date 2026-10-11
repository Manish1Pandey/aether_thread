// The web backend: same API, tasks on the UI thread. Run with
//   flutter test --platform chrome test/web_test.dart

@TestOn('browser')
library;

import 'dart:async';
import 'dart:typed_data';

import 'package:aether_thread/aether_thread.dart';
import 'package:flutter_test/flutter_test.dart';

final class _Frames implements AetherFrameScheduler {
  final List<void Function(Duration)> _transient = <void Function(Duration)>[];
  final List<void Function(Duration)> _post = <void Function(Duration)>[];

  @override
  bool framesEnabled = true;

  bool get pending => _transient.isNotEmpty;

  @override
  void scheduleFrameCallback(void Function(Duration) callback) =>
      _transient.add(callback);

  @override
  void addPostFrameCallback(void Function(Duration) callback) =>
      _post.add(callback);

  void pump() {
    final transient = List<void Function(Duration)>.of(_transient);
    _transient.clear();
    for (final callback in transient) {
      callback(Duration.zero);
    }
    final post = List<void Function(Duration)>.of(_post);
    _post.clear();
    for (final callback in post) {
      callback(Duration.zero);
    }
  }
}

Future<T> _pumpUntil<T>(_Frames frames, Future<T> future) async {
  var done = false;
  unawaited(
    future.then<void>((_) => done = true, onError: (Object _) => done = true),
  );
  final watch = Stopwatch()..start();
  while (!done) {
    if (watch.elapsed > const Duration(seconds: 10)) {
      throw TimeoutException('did not settle');
    }
    if (frames.pending) {
      frames.pump();
    }
    await Future<void>.delayed(const Duration(milliseconds: 1));
  }
  return future;
}

int Function() _add(int a, int b) =>
    () => a + b;
int _square(int x) => x * x;
int Function() _throws() =>
    () => throw StateError('web task failed');
Future<int> Function() _neverEnds() =>
    () => Completer<int>().future; // async work that never completes
Future<int> Function() _nested() =>
    () => AetherConcurrentRunner.computeAsync(_add(20, 22));
bool Function() _isTask() =>
    () => AetherWorkerPool.isWorkerIsolate;
Future<int> Function() _slow(int ms, int value) => () async {
  await Future<void>.delayed(Duration(milliseconds: ms));
  return value;
};

AetherProgressTask<int> _progress(int steps) => (report) async {
  for (var i = 1; i <= steps; i++) {
    await Future<void>.delayed(const Duration(milliseconds: 20));
    report(i / steps);
  }
  return steps;
};

void _double(Float32List slot, AetherPayload payload) {
  final lanes = slot.asFloat32x4();
  final two = Float32x4.splat(2);
  for (var i = 0; i < payload.floatCount ~/ 4; i++) {
    lanes[i] = lanes[i] * two;
  }
}

void _increment(Float32List previous, Float32List next, AetherPayload p) {
  for (var i = 0; i < p.floatCount; i++) {
    next[i] = previous[i] + 1;
  }
}

Future<void> _slowWrite(Float32List slot, AetherPayload payload) async {
  await Future<void>.delayed(const Duration(milliseconds: 60));
  slot[0] = 99;
}

Future<void> _writeOneAndHold(Float32List slot, AetherPayload payload) async {
  slot[0] = 1;
  await Future<void>.delayed(const Duration(milliseconds: 100));
}

void main() {
  late _Frames frames;
  late List<AetherFailure> failures;

  setUp(() async {
    frames = _Frames();
    failures = <AetherFailure>[];
    await AetherConcurrentRunner.initialize(
      config: const AetherConfig(slotCount: 4, slotStrideFloats: 64),
      onFailure: failures.add,
      frameScheduler: frames,
    );
  });

  tearDown(AetherConcurrentRunner.dispose);

  test('runs tasks with the same API, one "worker"', () async {
    expect(await AetherConcurrentRunner.computeAsync(_add(2, 3)), 5);
    expect(await AetherConcurrentRunner.computeAsync(_isTask()), isTrue);
    expect(AetherWorkerPool.isWorkerIsolate, isFalse);
    expect(AetherConcurrentRunner.stats().pool.workerCount, 1);
  });

  test('nested calls run inline instead of deadlocking', () async {
    expect(await AetherConcurrentRunner.computeAsync(_nested()), 42);
    expect(await AetherConcurrentRunner.computeOnce(_nested()), 42);
  });

  test('priorities order queued tasks', () async {
    final order = <String>[];
    final futures = <Future<void>>[
      AetherConcurrentRunner.computeAsync(
        _add(0, 0),
        priority: AetherPriority.low,
      ).then((_) => order.add('low')),
      AetherConcurrentRunner.computeAsync(
        _add(0, 0),
      ).then((_) => order.add('normal')),
      AetherConcurrentRunner.computeAsync(
        _add(0, 0),
        priority: AetherPriority.high,
      ).then((_) => order.add('high')),
    ];
    await Future.wait(futures);
    expect(order, <String>['high', 'normal', 'low']);
  });

  test('computeMap, computeOnce and progress work', () async {
    expect(
      await AetherConcurrentRunner.computeMap(
        List<int>.generate(100, (i) => i),
        _square,
      ),
      <int>[for (var i = 0; i < 100; i++) i * i],
    );
    expect(await AetherConcurrentRunner.computeOnce(_add(4, 4)), 8);
    final seen = <double>[];
    expect(
      await AetherConcurrentRunner.computeWithProgress(
        _progress(5),
        onProgress: seen.add,
      ),
      5,
    );
    expect(seen.last, 1.0);
  });

  test('task errors and timeouts are typed failures', () async {
    await expectLater(
      AetherConcurrentRunner.computeAsync(_throws()),
      throwsA(isA<AetherTaskException>()),
    );
    await expectLater(
      AetherConcurrentRunner.computeAsync(
        _neverEnds(),
        timeout: const Duration(milliseconds: 50),
      ),
      throwsA(isA<AetherTaskTimeout>()),
    );
    // The abandoned task does not block the queue.
    expect(await AetherConcurrentRunner.computeAsync(_add(1, 1)), 2);
    await expectLater(
      AetherConcurrentRunner.computeOnce(
        _neverEnds(),
        timeout: const Duration(milliseconds: 50),
      ),
      throwsA(isA<AetherTaskTimeout>()),
    );
    frames.pump();
    expect(failures.whereType<AetherTaskTimeout>(), hasLength(2));
  });

  test('cancel works on queued and running tasks', () async {
    final token = AetherCancelToken();
    final running = AetherConcurrentRunner.computeAsync(
      _slow(200, 1),
      cancelToken: token,
    );
    final queued = AetherConcurrentRunner.computeAsync(
      _slow(10, 2),
      cancelToken: token,
    );
    await Future<void>.delayed(const Duration(milliseconds: 20));
    token.cancel(hard: true);
    await expectLater(running, throwsA(isA<AetherTaskCancelled>()));
    await expectLater(queued, throwsA(isA<AetherTaskCancelled>()));
    expect(failures, isEmpty);
  });

  test('grids compute in place (SIMD) and apply in a frame', () async {
    Float32List? seen;
    final grid = AetherConcurrentRunner.computeGrid(
      kernel: _double,
      nodeCount: 4,
      prepare: (slot, payload) {
        for (var i = 0; i < payload.floatCount; i++) {
          slot[i] = i.toDouble();
        }
      },
      onApplied: (slot, payload) =>
          seen = Float32List.fromList(slot.sublist(0, payload.floatCount)),
    );
    await _pumpUntil(frames, grid);
    expect(seen, <double>[for (var i = 0; i < 16; i++) i * 2.0]);
    expect(AetherConcurrentRunner.stats().freeSlots, 4);
  });

  test('continuous grids step once per frame and stop cleanly', () async {
    final grid = await AetherConcurrentRunner.startContinuous(
      kernel: _increment,
      nodeCount: 2,
    );
    for (var step = 1; step <= 3; step++) {
      final deadline = Stopwatch()..start();
      while (!frames.pending) {
        if (deadline.elapsed > const Duration(seconds: 5)) {
          fail('no frame requested');
        }
        await Future<void>.delayed(const Duration(milliseconds: 1));
      }
      frames.pump();
      expect(grid.steps, step);
      expect(grid.notifier.value[0], step.toDouble());
    }
    await _pumpUntil(frames, grid.stop());
    expect(AetherConcurrentRunner.stats().freeSlots, 4);
  });

  test('a second runtime and dispose free everything', () async {
    final before = AetherMemoryStats.liveAllocations;
    final second = await AetherRuntime.start(
      config: const AetherConfig(slotCount: 2, slotStrideFloats: 16),
      frameScheduler: _Frames(),
    );
    expect(AetherMemoryStats.liveAllocations, before + 1);
    expect(await second.computeAsync(_add(6, 7)), 13);
    await second.dispose();
    expect(AetherMemoryStats.liveAllocations, before);
    expect(
      () => ContiguousMemoryBuffer.attach(
        const AetherBufferDescriptor(
          address: 0,
          slotCount: 1,
          slotStrideFloats: 16,
          nodeStride: 1,
        ),
      ),
      throwsUnsupportedError,
    );
  });

  test(
    'review #2: callbacks created by a task are not "nested" later',
    () async {
      final later = Completer<AetherPayload>();
      await AetherConcurrentRunner.computeAsync<int>(() {
        Timer(const Duration(milliseconds: 20), () {
          AetherConcurrentRunner.computeGrid(
            kernel: _double,
            nodeCount: 1,
          ).then(later.complete, onError: later.completeError);
        });
        return 0;
      });
      final payload = await _pumpUntil(frames, later.future);
      expect(payload.nodeCount, 1);
    },
  );

  test('review #2: workerInit callbacks are not "nested" later', () async {
    final seen = Completer<bool>();
    final runtime = await AetherRuntime.start(
      config: const AetherConfig(slotCount: 2, slotStrideFloats: 16),
      workerInit: () {
        Timer(
          const Duration(milliseconds: 10),
          () => seen.complete(AetherWorkerPool.isWorkerIsolate),
        );
      },
      frameScheduler: _Frames(),
    );
    expect(await seen.future, isFalse);
    await runtime.dispose();
  });

  test(
    'review #1: a timed-out grid kernel keeps its slot until it ends',
    () async {
      final local = _Frames();
      final runtime = await AetherRuntime.start(
        config: const AetherConfig(slotCount: 1, slotStrideFloats: 16),
        frameScheduler: local,
      );
      final watch = Stopwatch()..start();
      final first = runtime.computeGrid(
        kernel: _slowWrite,
        nodeCount: 1,
        timeout: const Duration(milliseconds: 10),
      );
      double? seenByNext;
      final second = runtime.computeGrid(
        kernel: _writeOneAndHold,
        nodeCount: 1,
        onApplied: (slot, payload) => seenByNext = slot[0],
      );
      await expectLater(first, throwsA(isA<AetherTaskTimeout>()));
      // Failed only once the kernel had stopped writing (after ~60 ms).
      expect(watch.elapsedMilliseconds, greaterThanOrEqualTo(55));
      await _pumpUntil(local, second);
      expect(
        seenByNext,
        1,
        reason: 'the abandoned kernel never wrote 99 over it',
      );
      await runtime.dispose();
    },
  );
}
