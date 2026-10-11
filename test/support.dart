// Shared helpers for the test suite.
//
// Task bodies sent to workers are built by top-level functions on purpose: a
// closure written inside a test body would capture that test's whole scope,
// which often holds unsendable objects such as the pool itself.

import 'dart:async';
import 'dart:io' show File, sleep;
import 'dart:isolate';
import 'dart:typed_data';

import 'package:aether_thread/aether_thread.dart';

/// Drives frames by hand so stitcher and runner tests are deterministic.
final class ManualFrameScheduler implements AetherFrameScheduler {
  final List<void Function(Duration)> _transient = <void Function(Duration)>[];
  final List<void Function(Duration)> _postFrame = <void Function(Duration)>[];
  int frames = 0;

  @override
  bool framesEnabled = true;

  /// Whether a frame has been requested and not yet pumped.
  bool get hasScheduledFrame => _transient.isNotEmpty;

  @override
  void scheduleFrameCallback(void Function(Duration timeStamp) callback) {
    _transient.add(callback);
  }

  @override
  void addPostFrameCallback(void Function(Duration timeStamp) callback) {
    _postFrame.add(callback);
  }

  /// Runs one frame: transient callbacks (before build), then post-frame
  /// callbacks (after paint), like the real scheduler.
  void pumpFrame() {
    frames++;
    final stamp = Duration(milliseconds: frames * 16);
    final transient = List<void Function(Duration)>.of(_transient);
    _transient.clear();
    for (final callback in transient) {
      callback(stamp);
    }
    final post = List<void Function(Duration)>.of(_postFrame);
    _postFrame.clear();
    for (final callback in post) {
      callback(stamp);
    }
  }
}

/// Waits (in real time) until [condition] holds, pumping nothing.
Future<void> waitUntil(
  bool Function() condition, {
  Duration timeout = const Duration(seconds: 10),
}) async {
  final watch = Stopwatch()..start();
  while (!condition()) {
    if (watch.elapsed > timeout) {
      throw TimeoutException('Condition not met', timeout);
    }
    await Future<void>.delayed(const Duration(milliseconds: 2));
  }
}

/// Keeps pumping frames until [future] completes.
Future<T> pumpUntilDone<T>(
  ManualFrameScheduler scheduler,
  Future<T> future, {
  Duration timeout = const Duration(seconds: 10),
}) async {
  var done = false;
  unawaited(
    future.then<void>((_) => done = true, onError: (Object _) => done = true),
  );
  final watch = Stopwatch()..start();
  while (!done) {
    if (watch.elapsed > timeout) {
      throw TimeoutException('Future did not complete', timeout);
    }
    if (scheduler.hasScheduledFrame) {
      scheduler.pumpFrame();
    }
    await Future<void>.delayed(const Duration(milliseconds: 1));
  }
  return future;
}

// ---- Task bodies -----------------------------------------------------------

/// Returns a task adding [a] and [b].
int Function() addTask(int a, int b) =>
    () => a + b;

/// Returns a task that sleeps [ms] on the worker, then returns [value].
int Function() sleepTask(int ms, int value) => () {
  sleep(Duration(milliseconds: ms));
  return value;
};

/// Returns a task that spins forever in Dart code (killable).
int Function() spinForeverTask() => () {
  var x = 0;
  while (true) {
    x = (x + 1) & 0xffff;
  }
};

/// Returns a task that throws [message].
int Function() throwTask(String message) =>
    () => throw StateError(message);

/// Returns a task that kills its own worker isolate.
int Function() suicideTask() => () {
  Isolate.current.kill(priority: Isolate.immediate);
  sleep(const Duration(seconds: 5));
  return 0;
};

/// Returns a task that raises an uncaught async error (fatal to the worker).
int Function() uncaughtAsyncErrorTask() => () {
  unawaited(Future<void>.error(StateError('uncaught on worker')));
  sleep(const Duration(milliseconds: 200));
  return 0;
};

/// Returns a task whose result cannot cross isolates.
Object Function() unsendableResultTask() => ReceivePort.new;

/// Returns a task that captures [port], which cannot cross isolates.
int Function() capturingTask(ReceivePort port) =>
    () => port.hashCode;

/// Returns a task that reports whether it runs on an Aether worker.
bool Function() isWorkerTask() =>
    () => AetherWorkerPool.isWorkerIsolate;

/// Returns a task that calls the runner from inside a worker.
Future<int> Function() nestedTask() =>
    () => AetherConcurrentRunner.computeAsync(addTask(20, 22));

/// Grid kernel: node i field f becomes `i * 10 + f` (+ whatever was there).
void fillKernel(Float32List slot, AetherPayload payload) {
  for (var node = 0; node < payload.nodeCount; node++) {
    for (var field = 0; field < payload.nodeStride; field++) {
      slot[node * payload.nodeStride + field] += node * 10 + field;
    }
  }
}

/// Grid kernel that doubles every used float.
void doubleKernel(Float32List slot, AetherPayload payload) {
  for (var i = 0; i < payload.floatCount; i++) {
    slot[i] *= 2;
  }
}

/// Grid kernel that sleeps [ms] first.
AetherGridKernel slowKernel(int ms) => (Float32List slot, AetherPayload p) {
  sleep(Duration(milliseconds: ms));
  slot[0] = 1;
};

/// Step kernel: next = previous + 1, element-wise.
void incrementStep(Float32List previous, Float32List next, AetherPayload p) {
  for (var i = 0; i < p.floatCount; i++) {
    next[i] = previous[i] + 1;
  }
}

/// Step kernel that always throws.
void failingStep(Float32List previous, Float32List next, AetherPayload p) {
  throw StateError('step failed');
}

/// Worker init that always fails.
void failingInit() => throw StateError('init failed');

/// Worker init that succeeds (sets a flag on the worker isolate).
bool workerInitRan = false;
void markingInit() {
  workerInitRan = true;
}

/// Returns a task that reports whether [markingInit] ran on this worker.
bool Function() initRanTask() =>
    () => workerInitRan;

/// Returns an isolate body that attaches to [descriptor], fills slot 2 with
/// `i * 1.5` for every used float and returns the header's task id.
int Function() attachAndFillTask(AetherBufferDescriptor descriptor) => () {
  final attached = ContiguousMemoryBuffer.attach(descriptor);
  final header = attached.readHeader(2);
  final view = attached.slotView(2);
  for (var i = 0; i < header.floatCount; i++) {
    view[i] = i * 1.5;
  }
  attached.dispose();
  return header.taskId;
};

/// Returns a task producing a [floats]-long list with `list[i] = i` sampled.
Float32List Function() bigResultTask(int floats) => () {
  final list = Float32List(floats);
  for (var i = 0; i < floats; i += 1024) {
    list[i] = i.toDouble();
  }
  return list;
};

/// Squares an int (top-level, so it is sendable as a map transform).
int square(int x) => x * x;

/// Returns a transform that throws on [bad] and doubles everything else.
int Function(int) failOn(int bad) => (int x) {
  if (x == bad) {
    throw StateError('bad item $x');
  }
  return x * 2;
};

/// Returns a transform that sleeps [ms] per item.
int Function(int) slowIdentity(int ms) => (int x) {
  sleep(Duration(milliseconds: ms));
  return x;
};

/// Returns a progress task that reports [steps] times, then returns [steps].
AetherProgressTask<int> progressTask(int steps, int stepMs) =>
    (void Function(double) report) {
      for (var i = 1; i <= steps; i++) {
        sleep(Duration(milliseconds: stepMs));
        report(i / steps);
      }
      return steps;
    };

/// Grid kernel doing `x = x * 2 + 1` four floats at a time.
void simdKernel(Float32List slot, AetherPayload payload) {
  final lanes = slot.asFloat32x4();
  final two = Float32x4.splat(2);
  final one = Float32x4.splat(1);
  for (var i = 0; i < payload.floatCount ~/ 4; i++) {
    lanes[i] = lanes[i] * two + one;
  }
}

/// Worker init that fails once the file at [path] exists, so a test can
/// break start-up after the pool is already running.
AetherWorkerInit failIfFileExists(String path) => () {
  if (File(path).existsSync()) {
    throw StateError('init broken by $path');
  }
};
