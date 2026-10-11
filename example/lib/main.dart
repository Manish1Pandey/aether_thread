// Aether Thread demo.
//
// A particle simulation runs either on an Aether worker (double-buffered,
// painted straight from shared memory) or on the UI thread, so you can watch
// the frame timings change. Buttons below exercise computeAsync and the
// failure paths.

import 'dart:async';
import 'dart:math' as math;
import 'dart:typed_data';

import 'package:aether_thread/aether_thread.dart';
import 'package:flutter/material.dart';
import 'package:flutter/scheduler.dart';

const List<int> particleCounts = <int>[20000, 100000, 300000];
const int maxParticles = 300000;

/// `flutter run --profile --dart-define=AETHER_AUTOBENCH=true` runs every
/// engine and particle count for a few seconds and prints frame timings.
const bool autoBench = bool.fromEnvironment('AETHER_AUTOBENCH');

Future<void> main() async {
  WidgetsFlutterBinding.ensureInitialized();
  await AetherConcurrentRunner.initialize(
    config: const AetherConfig(
      slotCount: 4,
      // Layout per slot: [x, y] × N, then [vx, vy] × N.
      slotStrideFloats: maxParticles * 4,
      nodeStride: 4,
    ),
    onFailure: (failure) => _failures.add(failure),
  );
  runApp(const AetherDemoApp());
}

final StreamController<AetherFailure> _failures =
    StreamController<AetherFailure>.broadcast();

// ---- Simulation ------------------------------------------------------------
//
// Top-level functions, so the closures sent to workers capture nothing from
// the widget tree.

/// Writes random positions and small velocities for `nodeCount` particles.
void seedParticles(Float32List slot, AetherPayload payload) {
  final n = payload.nodeCount;
  final random = math.Random(7);
  for (var i = 0; i < n; i++) {
    slot[2 * i] = random.nextDouble();
    slot[2 * i + 1] = random.nextDouble();
    slot[2 * n + 2 * i] = (random.nextDouble() - 0.5) * 0.002;
    slot[2 * n + 2 * i + 1] = (random.nextDouble() - 0.5) * 0.002;
  }
}

/// One step: every particle is pulled toward a well circling the centre and
/// bounces off the walls. Reads `previous`, writes `next`.
void stepParticles(Float32List previous, Float32List next, AetherPayload p) {
  final n = p.nodeCount;
  final t = p.taskId * 0.01;
  final wellX = 0.5 + 0.28 * math.cos(t);
  final wellY = 0.5 + 0.28 * math.sin(t * 1.3);
  for (var i = 0; i < n; i++) {
    final xi = 2 * i;
    final vi = 2 * n + 2 * i;
    var x = previous[xi];
    var y = previous[xi + 1];
    var vx = previous[vi];
    var vy = previous[vi + 1];
    final dx = wellX - x;
    final dy = wellY - y;
    final inv = 0.00002 / (dx * dx + dy * dy + 0.01);
    vx = (vx + dx * inv) * 0.999;
    vy = (vy + dy * inv) * 0.999;
    x += vx;
    y += vy;
    if (x < 0 || x > 1) {
      vx = -vx;
      x = x.clamp(0, 1);
    }
    if (y < 0 || y > 1) {
      vy = -vy;
      y = y.clamp(0, 1);
    }
    next[xi] = x;
    next[xi + 1] = y;
    next[vi] = vx;
    next[vi + 1] = vy;
  }
}

/// Counts primes below [limit] the slow way, to have real work to offload.
int Function() countPrimesTask(int limit) => () {
  var count = 0;
  for (var n = 2; n < limit; n++) {
    var prime = true;
    for (var d = 2; d * d <= n; d++) {
      if (n % d == 0) {
        prime = false;
        break;
      }
    }
    if (prime) {
      count++;
    }
  }
  return count;
};

/// Spins forever, to show a timeout being enforced.
int Function() spinTask() => () {
  var x = 0;
  while (true) {
    x = (x + 1) & 0xff;
  }
};

/// Scores one number with deliberately heavy math (for the map demo).
double heavyScore(int x) {
  var v = x.toDouble();
  for (var i = 0; i < 2000; i++) {
    v = math.sqrt(v * v + i) * 0.999;
  }
  return v;
}

/// Counts to [rows], reporting progress, to show computeWithProgress.
AetherProgressTask<int> importTask(int rows) => (report) {
  var checksum = 0;
  for (var i = 0; i < rows; i++) {
    checksum = (checksum + heavyScore(i).floor()) & 0xffffff;
    if (i % 500 == 0) {
      report(i / rows);
    }
  }
  report(1);
  return checksum;
};

/// Builds a 64 MB image-like buffer, to show computeOnce.
Uint8List Function() bigImageTask() => () {
  final bytes = Uint8List(64 * 1024 * 1024);
  for (var i = 0; i < bytes.length; i += 4096) {
    bytes[i] = i & 0xff;
  }
  return bytes;
};

/// Throws, to show a task failure.
int Function() throwingTask() =>
    () => throw StateError('Deliberate failure');

// ---- App -------------------------------------------------------------------

/// Root widget.
class AetherDemoApp extends StatelessWidget {
  /// Creates the app.
  const AetherDemoApp({super.key});

  @override
  Widget build(BuildContext context) => MaterialApp(
    title: 'Aether Thread',
    debugShowCheckedModeBanner: false,
    theme: ThemeData(
      colorSchemeSeed: const Color(0xFF6C5CE7),
      brightness: Brightness.dark,
      useMaterial3: true,
    ),
    home: const DemoPage(),
  );
}

enum _Engine { worker, uiThread }

/// The demo screen.
class DemoPage extends StatefulWidget {
  /// Creates the demo screen.
  const DemoPage({super.key});

  @override
  State<DemoPage> createState() => _DemoPageState();
}

class _DemoPageState extends State<DemoPage>
    with SingleTickerProviderStateMixin {
  _Engine _engine = _Engine.worker;
  int _count = 100000;
  AetherContinuousGrid? _grid;
  AetherSlotNotifier? _uiNotifier;
  Float32List? _uiFront;
  Float32List? _uiBack;
  int _uiStep = 0;
  late final Ticker _uiTicker = createTicker(_onUiTick);
  final _FrameStats _frames = _FrameStats();
  StreamSubscription<AetherFailure>? _failureSub;
  String _taskResult = 'Tap a button to run a task.';
  double? _progress;
  bool _switching = false;

  @override
  void initState() {
    super.initState();
    SchedulerBinding.instance.addTimingsCallback(_frames.add);
    _failureSub = _failures.stream.listen((failure) {
      if (!mounted) {
        return;
      }
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(content: Text('onFailure: ${failure.runtimeType}')),
      );
    });
    unawaited(_start().then((_) => autoBench ? _autoBench() : null));
  }

  Future<void> _autoBench() async {
    for (final count in <int>[100000, 300000]) {
      for (final engine in _Engine.values) {
        _count = count;
        _engine = engine;
        await _start();
        // Let the first frames settle, then measure a fresh window.
        await Future<void>.delayed(const Duration(seconds: 2));
        _frames.reset();
        await Future<void>.delayed(const Duration(seconds: 4));
        debugPrint(
          'AUTOBENCH ${engine.name} ${count ~/ 1000}k: '
          'build avg ${_frames.buildAvg.toStringAsFixed(2)} ms, '
          'max ${_frames.buildMax.toStringAsFixed(2)} ms, '
          'raster avg ${_frames.rasterAvg.toStringAsFixed(2)} ms, '
          'over 8.3 ms ${_frames.overBudget}/${_frames.frames} frames, '
          '${_frames.fps.toStringAsFixed(1)} fps'
          '${_grid == null ? '' : ', worker steps ${_grid!.steps}'}',
        );
      }
    }
    debugPrint('AUTOBENCH done');
  }

  @override
  void dispose() {
    SchedulerBinding.instance.removeTimingsCallback(_frames.add);
    unawaited(_failureSub?.cancel());
    _uiTicker.dispose();
    unawaited(_grid?.stop());
    _uiNotifier?.dispose();
    super.dispose();
  }

  Future<void> _start() async {
    if (_switching) {
      return;
    }
    _switching = true;
    _uiTicker.stop();
    final oldGrid = _grid;
    _grid = null;
    await oldGrid?.stop();
    oldGrid?.notifier.dispose();
    _uiNotifier?.dispose();
    _uiNotifier = null;
    _frames.reset();
    if (_engine == _Engine.worker) {
      _grid = await AetherConcurrentRunner.startContinuous(
        kernel: stepParticles,
        nodeCount: _count,
        seed: seedParticles,
      );
    } else {
      final payload = AetherPayload(
        taskId: 0,
        slotIndex: 0,
        nodeCount: _count,
        nodeStride: 4,
      );
      _uiFront = Float32List(_count * 4);
      _uiBack = Float32List(_count * 4);
      seedParticles(_uiFront!, payload);
      _uiNotifier = AetherSlotNotifier(_uiFront!, payload);
      _uiStep = 0;
      _uiTicker.start();
    }
    _switching = false;
    if (mounted) {
      setState(() {});
    }
  }

  /// UI-thread mode: the same kernel, run synchronously every frame.
  void _onUiTick(Duration _) {
    final front = _uiFront!;
    final back = _uiBack!;
    final payload = AetherPayload(
      taskId: ++_uiStep,
      slotIndex: 0,
      nodeCount: _count,
      nodeStride: 4,
    );
    stepParticles(front, back, payload);
    _uiFront = back;
    _uiBack = front;
    _uiNotifier!.publish(back, payload);
  }

  Future<void> _runPrimes() async {
    setState(() => _taskResult = 'Counting primes below 3,000,000…');
    final watch = Stopwatch()..start();
    try {
      final primes = await AetherConcurrentRunner.computeAsync(
        countPrimesTask(3000000),
      );
      _setResult('$primes primes in ${watch.elapsedMilliseconds} ms');
    } on AetherFailure catch (failure) {
      _setResult('Failed: $failure');
    }
  }

  Future<void> _runTimeout() async {
    _setResult('Running a task that never ends, with a 300 ms timeout…');
    try {
      await AetherConcurrentRunner.computeAsync(
        spinTask(),
        timeout: const Duration(milliseconds: 300),
      );
    } on AetherTaskTimeout catch (failure) {
      _setResult('Timed out and the worker was respawned: ${failure.message}');
    }
  }

  Future<void> _runMap() async {
    _setResult('Scoring 100,000 items on every worker…');
    final watch = Stopwatch()..start();
    final scores = await AetherConcurrentRunner.computeMap(
      List<int>.generate(100000, (i) => i),
      heavyScore,
    );
    _setResult(
      'computeMap: ${scores.length} scores in ${watch.elapsedMilliseconds} ms '
      'on ${AetherConcurrentRunner.stats().pool.workerCount} workers',
    );
  }

  Future<void> _runProgress() async {
    setState(() => _progress = 0);
    final checksum = await AetherConcurrentRunner.computeWithProgress(
      importTask(20000),
      onProgress: (fraction) => setState(() => _progress = fraction),
    );
    _setResult('computeWithProgress done, checksum $checksum');
    setState(() => _progress = null);
  }

  Future<void> _runOnce() async {
    final watch = Stopwatch()..start();
    final bytes = await AetherConcurrentRunner.computeOnce(bigImageTask());
    _setResult(
      'computeOnce: ${bytes.lengthInBytes ~/ (1024 * 1024)} MB handed over '
      'without a copy in ${watch.elapsedMilliseconds} ms',
    );
  }

  Future<void> _runThrow() async {
    try {
      await AetherConcurrentRunner.computeAsync(throwingTask());
    } on AetherTaskException catch (failure) {
      _setResult('Caught on the UI: ${failure.error}');
    }
  }

  void _setResult(String text) {
    if (mounted) {
      setState(() => _taskResult = text);
    }
  }

  @override
  Widget build(BuildContext context) {
    final notifier = _grid?.notifier ?? _uiNotifier;
    final theme = Theme.of(context);
    return Scaffold(
      appBar: AppBar(title: const Text('Aether Thread')),
      body: SafeArea(
        child: Column(
          children: <Widget>[
            Padding(
              padding: const EdgeInsets.fromLTRB(16, 8, 16, 0),
              child: Wrap(
                spacing: 12,
                runSpacing: 8,
                crossAxisAlignment: WrapCrossAlignment.center,
                children: <Widget>[
                  SegmentedButton<_Engine>(
                    segments: const <ButtonSegment<_Engine>>[
                      ButtonSegment(
                        value: _Engine.worker,
                        label: Text('Aether worker'),
                      ),
                      ButtonSegment(
                        value: _Engine.uiThread,
                        label: Text('UI thread'),
                      ),
                    ],
                    selected: <_Engine>{_engine},
                    onSelectionChanged: (selection) {
                      _engine = selection.single;
                      unawaited(_start());
                    },
                  ),
                  DropdownButton<int>(
                    value: _count,
                    items: <DropdownMenuItem<int>>[
                      for (final count in particleCounts)
                        DropdownMenuItem(
                          value: count,
                          child: Text('${count ~/ 1000}k particles'),
                        ),
                    ],
                    onChanged: (count) {
                      if (count == null) {
                        return;
                      }
                      _count = count;
                      unawaited(_start());
                    },
                  ),
                ],
              ),
            ),
            Expanded(
              child: Stack(
                children: <Widget>[
                  Positioned.fill(
                    child: RepaintBoundary(
                      child: notifier == null
                          ? const Center(child: CircularProgressIndicator())
                          : CustomPaint(
                              painter: AetherSlotPainter(
                                notifier: notifier,
                                pointCount: _count,
                                color: theme.colorScheme.primary,
                                pointSize: 1.2,
                              ),
                            ),
                    ),
                  ),
                  Positioned(
                    left: 16,
                    top: 12,
                    child: _FrameOverlay(stats: _frames, grid: _grid),
                  ),
                ],
              ),
            ),
            Padding(
              padding: const EdgeInsets.all(16),
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.stretch,
                children: <Widget>[
                  Text(_taskResult, style: theme.textTheme.bodyMedium),
                  if (_progress != null) ...<Widget>[
                    const SizedBox(height: 8),
                    LinearProgressIndicator(value: _progress),
                  ],
                  const SizedBox(height: 8),
                  Wrap(
                    spacing: 8,
                    runSpacing: 8,
                    children: <Widget>[
                      FilledButton(
                        onPressed: _runPrimes,
                        child: const Text('Count primes'),
                      ),
                      OutlinedButton(
                        onPressed: _runTimeout,
                        child: const Text('Timeout demo'),
                      ),
                      OutlinedButton(
                        onPressed: _runThrow,
                        child: const Text('Failure demo'),
                      ),
                      OutlinedButton(
                        onPressed: _runMap,
                        child: const Text('Parallel map'),
                      ),
                      OutlinedButton(
                        onPressed: _runProgress,
                        child: const Text('Progress'),
                      ),
                      OutlinedButton(
                        onPressed: _runOnce,
                        child: const Text('64 MB result'),
                      ),
                    ],
                  ),
                ],
              ),
            ),
          ],
        ),
      ),
    );
  }
}

/// Rolling window of the engine's own frame timings.
class _FrameStats extends ChangeNotifier {
  static const int window = 120;
  final List<FrameTiming> _timings = <FrameTiming>[];
  final Stopwatch _since = Stopwatch()..start();
  int _received = 0;

  void reset() {
    _timings.clear();
    _received = 0;
    _since.reset();
    notifyListeners();
  }

  /// Frames rendered per second since the last [reset].
  double get fps => _since.elapsedMicroseconds == 0
      ? 0
      : _received * 1e6 / _since.elapsedMicroseconds;

  void add(List<FrameTiming> timings) {
    _received += timings.length;
    _timings.addAll(timings);
    if (_timings.length > window) {
      _timings.removeRange(0, _timings.length - window);
    }
    notifyListeners();
  }

  double _avgMs(Duration Function(FrameTiming) pick) => _timings.isEmpty
      ? 0
      : _timings.fold<int>(0, (a, t) => a + pick(t).inMicroseconds) /
            _timings.length /
            1000;

  double _maxMs(Duration Function(FrameTiming) pick) => _timings.isEmpty
      ? 0
      : _timings.map((t) => pick(t).inMicroseconds).reduce(math.max) / 1000;

  double get buildAvg => _avgMs((t) => t.buildDuration);
  double get buildMax => _maxMs((t) => t.buildDuration);
  double get rasterAvg => _avgMs((t) => t.rasterDuration);
  int get frames => _timings.length;

  /// Frames whose build or raster phase overran a 120 Hz budget (8.3 ms).
  int get overBudget => _timings
      .where(
        (t) =>
            t.buildDuration.inMicroseconds > 8333 ||
            t.rasterDuration.inMicroseconds > 8333,
      )
      .length;
}

class _FrameOverlay extends StatelessWidget {
  const _FrameOverlay({required this.stats, required this.grid});

  final _FrameStats stats;
  final AetherContinuousGrid? grid;

  @override
  Widget build(BuildContext context) => ListenableBuilder(
    listenable: stats,
    builder: (context, _) {
      final text =
          'UI build  avg ${stats.buildAvg.toStringAsFixed(2)} ms · '
          'max ${stats.buildMax.toStringAsFixed(2)} ms\n'
          'raster    avg ${stats.rasterAvg.toStringAsFixed(2)} ms\n'
          'over 8.3 ms budget: ${stats.overBudget}/${stats.frames} frames\n'
          'frame rate: ${stats.fps.toStringAsFixed(1)} fps'
          '${grid == null ? '' : '\nworker steps: ${grid!.steps}'}';
      return DecoratedBox(
        decoration: BoxDecoration(
          color: Colors.black.withValues(alpha: 0.6),
          borderRadius: BorderRadius.circular(8),
        ),
        child: Padding(
          padding: const EdgeInsets.all(10),
          child: Text(
            text,
            style: const TextStyle(
              fontFamily: 'monospace',
              fontSize: 12,
              color: Colors.white,
            ),
          ),
        ),
      );
    },
  );
}
