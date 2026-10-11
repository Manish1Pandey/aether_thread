@TestOn('vm')
library;

import 'dart:isolate';
import 'dart:typed_data';

import 'package:aether_thread/aether_thread.dart';
import 'package:flutter_test/flutter_test.dart';

import 'support.dart';

const _config = AetherConfig(
  pool: AetherPoolConfig(
    workerCount: 3,
    watchdogTick: Duration(milliseconds: 5),
  ),
  slotCount: 4,
  slotStrideFloats: 256,
);

void main() {
  late ManualFrameScheduler frames;
  late List<AetherFailure> failures;

  setUp(() async {
    frames = ManualFrameScheduler();
    failures = <AetherFailure>[];
    await AetherConcurrentRunner.initialize(
      config: _config,
      onFailure: failures.add,
      frameScheduler: frames,
    );
  });

  tearDown(AetherConcurrentRunner.dispose);

  group('computeOnce', () {
    test('returns a large result handed over from a fresh isolate', () async {
      final list = await AetherConcurrentRunner.computeOnce(
        bigResultTask(4 * 1024 * 1024),
      );
      expect(list.length, 4 * 1024 * 1024);
      expect(list[1024 * 7], 1024 * 7);
      expect(AetherConcurrentRunner.stats().oneShotRunning, 0);
    });

    test('times out by killing the isolate', () async {
      await expectLater(
        AetherConcurrentRunner.computeOnce(
          spinForeverTask(),
          timeout: const Duration(milliseconds: 80),
        ),
        throwsA(isA<AetherTaskTimeout>()),
      );
      frames.pumpFrame();
      expect(failures.single, isA<AetherTaskTimeout>());
    });

    test('cancels, reports task errors, crashes and unsendables', () async {
      final token = AetherCancelToken();
      final cancelled = AetherConcurrentRunner.computeOnce(
        spinForeverTask(),
        cancelToken: token,
      );
      await Future<void>.delayed(const Duration(milliseconds: 30));
      token.cancel();
      await expectLater(cancelled, throwsA(isA<AetherTaskCancelled>()));
      await expectLater(
        AetherConcurrentRunner.computeOnce(throwTask('once')),
        throwsA(isA<AetherTaskException>()),
      );
      await expectLater(
        AetherConcurrentRunner.computeOnce(suicideTask()),
        throwsA(isA<AetherWorkerCrash>()),
      );
      await expectLater(
        AetherConcurrentRunner.computeOnce(unsendableResultTask()),
        throwsA(isA<AetherUnsendableResult>()),
      );
      final port = ReceivePort();
      await expectLater(
        AetherConcurrentRunner.computeOnce(capturingTask(port)),
        throwsA(isA<AetherUnsendableTask>()),
      );
      port.close();
      expect(AetherConcurrentRunner.stats().oneShotRunning, 0);
    });

    test('dispose kills running one-shot work', () async {
      final running = AetherConcurrentRunner.computeOnce(spinForeverTask());
      final fails = expectLater(running, throwsA(isA<AetherPoolShutdown>()));
      await Future<void>.delayed(const Duration(milliseconds: 30));
      await AetherConcurrentRunner.dispose();
      await fails;
    });
  });

  group('computeMap', () {
    test('maps across workers and keeps the order', () async {
      final items = List<int>.generate(1000, (i) => i);
      final squares = await AetherConcurrentRunner.computeMap(items, square);
      expect(squares, <int>[for (final i in items) i * i]);
      final odd = await AetherConcurrentRunner.computeMap(
        List<int>.generate(10, (i) => i),
        square,
        chunkSize: 3,
      );
      expect(odd, <int>[0, 1, 4, 9, 16, 25, 36, 49, 64, 81]);
      expect(await AetherConcurrentRunner.computeMap(<int>[], square), isEmpty);
    });

    test('one failing item fails the map and cancels the rest', () async {
      await expectLater(
        AetherConcurrentRunner.computeMap(
          List<int>.generate(400, (i) => i),
          failOn(5),
          chunkSize: 10,
        ),
        throwsA(
          isA<AetherTaskException>().having(
            (f) => '${f.error}',
            'error',
            contains('bad item 5'),
          ),
        ),
      );
      await waitUntil(() => AetherConcurrentRunner.stats().pool.queued == 0);
    });

    test('a cancel token cancels the whole map', () async {
      final token = AetherCancelToken();
      final mapping = AetherConcurrentRunner.computeMap(
        List<int>.generate(60, (i) => i),
        slowIdentity(5),
        chunkSize: 2,
        cancelToken: token,
      );
      await Future<void>.delayed(const Duration(milliseconds: 20));
      token.cancel();
      await expectLater(mapping, throwsA(isA<AetherTaskCancelled>()));
    });

    test('rejects a zero chunk size', () async {
      await expectLater(
        AetherConcurrentRunner.computeMap(<int>[1], square, chunkSize: 0),
        throwsRangeError,
      );
    });
  });

  test('computeWithProgress reports rising progress ending at 1.0', () async {
    final seen = <double>[];
    final result = await AetherConcurrentRunner.computeWithProgress(
      progressTask(20, 5),
      onProgress: seen.add,
    );
    expect(result, 20);
    expect(seen, isNotEmpty);
    expect(seen.last, 1.0);
    for (var i = 1; i < seen.length; i++) {
      expect(seen[i], greaterThanOrEqualTo(seen[i - 1]));
    }
    // Throttled: 20 reports 5 ms apart cannot all get through at 16 ms.
    expect(seen.length, lessThan(20));
  });

  test(
    'unsendable errors name the captured class and explain the fix',
    () async {
      final port = ReceivePort();
      try {
        await AetherConcurrentRunner.computeAsync(capturingTask(port));
        fail('should not send');
      } on AetherUnsendableTask catch (failure) {
        expect(failure.message, contains('dart:isolate'));
        expect(failure.message, contains('top-level or static function'));
      } finally {
        port.close();
      }
    },
  );

  test('SIMD views share memory with the slot, also on a worker', () async {
    Float32List? seen;
    final grid = AetherConcurrentRunner.computeGrid(
      kernel: simdKernel,
      nodeCount: 4,
      prepare: (slot, payload) {
        for (var i = 0; i < payload.floatCount; i++) {
          slot[i] = i.toDouble();
        }
        expect(slot.asFloat32x4()[1].y, 5);
        expect(slot.asFloat64().length, slot.length ~/ 2);
        expect(slot.asBytes().length, slot.length * 4);
      },
      onApplied: (slot, payload) =>
          seen = Float32List.fromList(slot.sublist(0, payload.floatCount)),
    );
    await pumpUntilDone(frames, grid);
    expect(seen, <double>[for (var i = 0; i < 16; i++) i * 2.0 + 1]);
  });

  group('AetherRuntime', () {
    test('independent runtimes run side by side', () async {
      final small = await AetherRuntime.start(
        config: const AetherConfig(
          pool: AetherPoolConfig(workerCount: 1),
          slotCount: 2,
          slotStrideFloats: 16,
        ),
        frameScheduler: ManualFrameScheduler(),
      );
      expect(small.stats().pool.workerCount, 1);
      expect(AetherConcurrentRunner.stats().pool.workerCount, 3);
      expect(await small.computeAsync(addTask(1, 2)), 3);
      expect(await AetherConcurrentRunner.computeAsync(addTask(3, 4)), 7);
      await small.dispose();
      expect(small.isDisposed, isTrue);
      expect(() => small.computeAsync(addTask(1, 1)), throwsStateError);
      expect(await AetherConcurrentRunner.computeAsync(addTask(5, 5)), 10);
    });

    test('the default runtime is reachable and updates onFailure', () async {
      expect(AetherConcurrentRunner.runtime.config, _config);
      final later = <AetherFailure>[];
      await AetherConcurrentRunner.initialize(
        config: _config,
        onFailure: later.add,
        frameScheduler: frames,
      );
      await expectLater(
        AetherConcurrentRunner.computeAsync(throwTask('x')),
        throwsA(isA<AetherTaskException>()),
      );
      frames.pumpFrame();
      expect(later, hasLength(1));
      expect(failures, isEmpty);
    });
  });
}
