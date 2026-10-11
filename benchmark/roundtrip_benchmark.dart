// Aether pool vs Isolate.run (what Flutter's compute() uses on native).
//
// Run it AOT-compiled, like a release app:
//   dart compile exe benchmark/roundtrip_benchmark.dart -o benchmark/roundtrip_benchmark
//   ./benchmark/roundtrip_benchmark
//
// For each workload it reports wall time and the longest stall of the main
// event loop (a 1 ms ticker), which is what turns into dropped frames.

import 'dart:async';
import 'dart:io';
import 'dart:isolate';
import 'dart:math' as math;
import 'dart:typed_data';

import 'package:aether_thread/src/aether_memory_grid.dart';
import 'package:aether_thread/src/aether_one_shot.dart';
import 'package:aether_thread/src/aether_pool_manager.dart';

const int _smallRounds = 2000;
const int _bigFloats = 8 * 1024 * 1024; // 32 MB
const int _gridFloats = 1024 * 1024; // 4 MB
const int _gridRounds = 50;

int _add(int a, int b) => a + b;
int Function() _addTask(int i) =>
    () => _add(i, 1);

Float32List Function() _bigResultTask() => () {
  final list = Float32List(_bigFloats);
  for (var i = 0; i < list.length; i += 4096) {
    list[i] = i.toDouble();
  }
  return list;
};

void _gridKernel(Float32List slot, AetherPayload payload) {
  for (var i = 0; i < payload.floatCount; i++) {
    slot[i] = slot[i] * 1.0001 + 1;
  }
}

void _simdKernel(Float32List slot, AetherPayload payload) {
  final lanes = slot.asFloat32x4();
  final scale = Float32x4.splat(1.0001);
  final one = Float32x4.splat(1);
  for (var i = 0; i < payload.floatCount ~/ 4; i++) {
    lanes[i] = lanes[i] * scale + one;
  }
}

/// CPU-heavy per-item work for the parallel map rows.
double _heavy(int x) {
  var v = x.toDouble();
  for (var i = 0; i < 400; i++) {
    v = math.sqrt(v * v + i) * 0.999;
  }
  return v;
}

List<double> Function() _mapChunk(List<int> chunk) =>
    () => <double>[for (final x in chunk) _heavy(x)];

const int _mapItems = 200000;

Float32List Function() _copyKernelTask(Float32List input) => () {
  final out = Float32List(input.length);
  for (var i = 0; i < input.length; i++) {
    out[i] = input[i] * 1.0001 + 1;
  }
  return out;
};

/// Watches the main event loop and records its longest gap.
final class _StallMeter {
  int _last = 0;
  int maxGapMicros = 0;
  final Stopwatch _clock = Stopwatch()..start();
  Timer? _timer;

  void start() {
    maxGapMicros = 0;
    _last = _clock.elapsedMicroseconds;
    _timer = Timer.periodic(const Duration(milliseconds: 1), (_) {
      final now = _clock.elapsedMicroseconds;
      maxGapMicros = math.max(maxGapMicros, now - _last);
      _last = now;
    });
  }

  void stop() => _timer?.cancel();
}

final class _Result {
  _Result(this.name, this.samplesMicros, this.wallMicros, this.maxStallMicros);

  final String name;
  final List<int> samplesMicros;
  final int wallMicros;
  final int maxStallMicros;

  int _pct(double p) {
    if (samplesMicros.isEmpty) {
      return 0;
    }
    final sorted = List<int>.of(samplesMicros)..sort();
    return sorted[math.min(sorted.length - 1, (sorted.length * p).floor())];
  }

  String row() =>
      '${name.padRight(44)}'
      '${_fmt(_pct(0.5)).padLeft(10)}'
      '${_fmt(_pct(0.99)).padLeft(10)}'
      '${_fmt(wallMicros).padLeft(11)}'
      '${_fmt(maxStallMicros).padLeft(11)}';
}

String _fmt(int micros) =>
    micros >= 10000 ? '${(micros / 1000).toStringAsFixed(1)}ms' : '${micros}us';

Future<_Result> _measure(
  String name,
  int rounds,
  Future<void> Function(int i) once, {
  bool parallel = false,
}) async {
  final meter = _StallMeter()..start();
  final samples = <int>[];
  final wall = Stopwatch()..start();
  if (parallel) {
    await Future.wait(<Future<void>>[
      for (var i = 0; i < rounds; i++)
        () async {
          final sw = Stopwatch()..start();
          await once(i);
          samples.add(sw.elapsedMicroseconds);
        }(),
    ]);
  } else {
    for (var i = 0; i < rounds; i++) {
      final sw = Stopwatch()..start();
      await once(i);
      samples.add(sw.elapsedMicroseconds);
    }
  }
  wall.stop();
  meter.stop();
  return _Result(name, samples, wall.elapsedMicroseconds, meter.maxGapMicros);
}

Future<void> main() async {
  final workers = const AetherPoolConfig().resolvedWorkerCount;
  final buffer = ContiguousMemoryBuffer.allocate(
    slotCount: 2,
    slotStrideFloats: _gridFloats,
    nodeStride: 4,
  );
  final pool = await AetherWorkerPool.start(
    config: AetherPoolConfig(workerCount: workers, enableTimeline: false),
    buffer: buffer,
  );
  final oneShot = AetherOneShotRunner();
  // Warm both paths so JIT-free AOT code and isolate groups are settled.
  for (var i = 0; i < 50; i++) {
    await pool.run(_addTask(i));
    await Isolate.run(_addTask(i));
  }

  final results = <_Result>[
    await _measure(
      'small task, sequential — Aether',
      _smallRounds,
      (i) => pool.run(_addTask(i)),
    ),
    await _measure(
      'small task, sequential — Isolate.run',
      _smallRounds,
      (i) => Isolate.run(_addTask(i)),
    ),
    await _measure(
      'small task, $_smallRounds at once — Aether',
      _smallRounds,
      (i) => pool.run(_addTask(i)),
      parallel: true,
    ),
    await _measure(
      'small task, $_smallRounds at once — Isolate.run',
      _smallRounds,
      (i) => Isolate.run(_addTask(i)),
      parallel: true,
    ),
    await _measure(
      '32 MB result — Aether',
      10,
      (_) => pool.run(_bigResultTask()),
    ),
    await _measure(
      '32 MB result — Isolate.run',
      10,
      (_) => Isolate.run(_bigResultTask()),
    ),
    await _measure(
      '32 MB result — Aether computeOnce',
      10,
      (i) => oneShot.run(_bigResultTask(), taskId: i),
    ),
  ];

  // Grid: Aether transforms shared memory in place; the Isolate.run version
  // has to send the 4 MB input over and the 4 MB output back.
  final slot = buffer.acquireSlot();
  buffer.writeHeader(
    AetherPayload(
      taskId: 1,
      slotIndex: slot,
      nodeCount: _gridFloats ~/ 4,
      nodeStride: 4,
    ),
  );
  var heapCopy = Float32List(_gridFloats);
  results
    ..add(
      await _measure(
        '4 MB in-place kernel — Aether grid',
        _gridRounds,
        (_) => pool.runGrid(slot, _gridKernel),
      ),
    )
    ..add(
      await _measure(
        '4 MB in-place kernel, SIMD — Aether grid',
        _gridRounds,
        (_) => pool.runGrid(slot, _simdKernel),
      ),
    )
    ..add(
      await _measure(
        '4 MB kernel, copy in + out — Isolate.run',
        _gridRounds,
        (_) async => heapCopy = await Isolate.run(_copyKernelTask(heapCopy)),
      ),
    );

  // Parallel map: Aether splits across workers; Isolate.run and the main
  // thread each do the whole list on one core.
  final items = List<int>.generate(_mapItems, (i) => i);
  final chunk = (_mapItems / (workers * 4)).ceil();
  results
    ..add(
      await _measure('map 200k heavy items — Aether chunks', 3, (_) async {
        await Future.wait(<Future<List<double>>>[
          for (var start = 0; start < _mapItems; start += chunk)
            pool.run(
              _mapChunk(
                items.sublist(start, math.min(_mapItems, start + chunk)),
              ),
            ),
        ]);
      }),
    )
    ..add(
      await _measure(
        'map 200k heavy items — one Isolate.run',
        3,
        (_) => Isolate.run(_mapChunk(items)),
      ),
    )
    ..add(
      await _measure('map 200k heavy items — main thread', 3, (_) async {
        _mapChunk(items)();
      }),
    );

  stdout
    ..writeln(
      'AOT, ${Platform.operatingSystem}, ${Platform.numberOfProcessors} '
      'cores, $workers Aether workers',
    )
    ..writeln(
      '${'workload'.padRight(44)}${'p50'.padLeft(10)}${'p99'.padLeft(10)}'
      '${'wall'.padLeft(11)}${'max stall'.padLeft(11)}',
    );
  for (final result in results) {
    stdout.writeln(result.row());
  }

  await pool.shutdown();
  buffer.dispose();
}
