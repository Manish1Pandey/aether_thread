// Web backend of the worker pool.
//
// The web has no threads that can run Dart closures (no `dart:isolate`, and
// Web Workers cannot receive closures), so this backend runs tasks on the UI
// thread, one per event-loop turn, with the same API, queues, priorities,
// aging, progress, cancellation and typed failures as the native pool.
// Frames are drawn between tasks, but a long synchronous task still blocks
// the page while it runs. A timeout or hard cancel abandons the task: its
// future fails at once and its eventual result is discarded, but code that
// is already running cannot be stopped.

import 'dart:async';
import 'dart:collection';
import 'dart:developer' show TimelineTask;
import 'dart:math' as math;

import 'aether_deadline.dart';
import 'aether_failures.dart';
import 'aether_memory_grid_web.dart';
import 'aether_pool_types.dart';

export 'aether_pool_types.dart';

/// Zone key marking code that runs as an Aether task, so nested runner calls
/// run inline instead of queueing behind themselves.
final Object aetherWorkerZoneKey = Object();

/// The zone value under [aetherWorkerZoneKey]. Timers and listeners created
/// inside a task inherit its zone, so the marker is switched off when the
/// task ends: callbacks firing later are ordinary app code, not nested calls.
final class AetherTaskMarker {
  /// Whether the task that owns this marker is still running.
  bool active = true;
}

Map<Object?, Object?> _markedZone(AetherTaskMarker marker) =>
    <Object?, Object?>{aetherWorkerZoneKey: marker};

const int _kindValue = 0;
const int _kindGrid = 1;
const int _kindStep = 2;
const int _kindProgress = 3;

enum _TaskState { queued, running, done }

final class _WebTask {
  _WebTask(
    this.id,
    this.kind,
    this.body,
    this.slotA,
    this.slotB,
    this.level,
    this.timeout,
    this.submittedAtMicros,
    this.cancelToken,
    this.timeline,
  );

  final int id;
  final int kind;
  final Function body;
  final int slotA;
  final int slotB;
  final int level;
  final Duration? timeout;
  final int submittedAtMicros;
  final AetherCancelToken? cancelToken;
  final TimelineTask? timeline;
  final Completer<Object?> completer = Completer<Object?>();
  void Function(bool hard)? cancelListener;
  void Function(double fraction)? onProgress;
  AetherDeadline? deadline;
  final AetherTaskMarker marker = AetherTaskMarker();
  // A timeout or cancel waiting for a grid or step kernel to finish.
  AetherFailure? pendingFailure;
  _TaskState state = _TaskState.queued;
  bool softCancelled = false;
}

/// The web pool: the same API as the native pool, running tasks on the UI
/// thread one at a time.
final class AetherWorkerPool {
  AetherWorkerPool._(this.config, this._buffer)
    : _latency = List<int>.filled(math.max(1, config.latencySampleCapacity), 0);

  /// Creates the pool and runs [workerInit] once. Throws
  /// [AetherWorkerInitFailure] if it fails.
  static Future<AetherWorkerPool> start({
    AetherPoolConfig config = const AetherPoolConfig(),
    ContiguousMemoryBuffer? buffer,
    AetherWorkerInit? workerInit,
  }) async {
    final pool = AetherWorkerPool._(config, buffer);
    if (workerInit != null) {
      final marker = AetherTaskMarker();
      try {
        await runZoned(
          () => Future<void>.sync(workerInit),
          zoneValues: _markedZone(marker),
        );
      } catch (error, stack) {
        throw AetherWorkerInitFailure(0, 'workerInit failed: $error\n$stack');
      } finally {
        marker.active = false;
      }
    }
    return pool;
  }

  /// Whether the current code is running as an Aether task.
  static bool get isWorkerIsolate {
    final marker = Zone.current[aetherWorkerZoneKey];
    return marker is AetherTaskMarker && marker.active;
  }

  /// The configuration this pool was started with.
  final AetherPoolConfig config;

  /// Called for every failure except [AetherTaskCancelled] and
  /// [AetherPoolShutdown], which the caller caused.
  void Function(AetherFailure failure)? onFailure;

  final ContiguousMemoryBuffer? _buffer;
  final Zone _zone = Zone.current;
  final Stopwatch _clock = Stopwatch()..start();
  final List<ListQueue<_WebTask>> _queues = List<ListQueue<_WebTask>>.generate(
    AetherPriority.values.length,
    (_) => ListQueue<_WebTask>(64),
    growable: false,
  );
  final List<int> _queuedCounts = List<int>.filled(
    AetherPriority.values.length,
    0,
  );
  final List<int> _latency;
  int _latencyCount = 0;
  int _latencyNext = 0;
  int _nextTaskId = 1;
  int _completed = 0;
  int _failed = 0;
  bool _shuttingDown = false;
  bool _watchdogPaused = false;
  bool _dispatchScheduled = false;
  _WebTask? _running;

  /// Whether [shutdown] has been called.
  bool get isShutDown => _shuttingDown;

  /// Always 1: tasks run on the UI thread, one at a time.
  int get workerCount => 1;

  /// Reserves a task id, so a caller can write it into a payload header
  /// before submitting.
  int reserveTaskId() => _nextTaskId++;

  /// Runs [task] and completes with its result.
  Future<T> run<T>(
    FutureOr<T> Function() task, {
    AetherPriority priority = AetherPriority.normal,
    Duration? timeout,
    AetherCancelToken? cancelToken,
    int? taskId,
  }) => _submit(
    _kindValue,
    task,
    -1,
    -1,
    priority,
    timeout,
    cancelToken,
    taskId,
  ).then(_cast<T>);

  static T _cast<T>(Object? value) => value as T;

  /// Like [run], for a task that reports progress through [onProgress].
  Future<T> runWithProgress<T>(
    AetherProgressTask<T> task, {
    required void Function(double fraction) onProgress,
    AetherPriority priority = AetherPriority.normal,
    Duration? timeout,
    AetherCancelToken? cancelToken,
    int? taskId,
  }) => _submit(
    _kindProgress,
    task,
    -1,
    -1,
    priority,
    timeout,
    cancelToken,
    taskId,
    onProgress,
  ).then(_cast<T>);

  /// Runs [kernel] over [slot] of the buffer.
  Future<void> runGrid(
    int slot,
    AetherGridKernel kernel, {
    AetherPriority priority = AetherPriority.normal,
    Duration? timeout,
    AetherCancelToken? cancelToken,
    int? taskId,
  }) => _submit(
    _kindGrid,
    kernel,
    slot,
    -1,
    priority,
    timeout,
    cancelToken,
    taskId,
  );

  /// Runs [kernel] reading [previousSlot] and writing [nextSlot].
  Future<void> runStep(
    int nextSlot,
    int previousSlot,
    AetherStepKernel kernel, {
    AetherPriority priority = AetherPriority.normal,
    Duration? timeout,
    AetherCancelToken? cancelToken,
    int? taskId,
  }) => _submit(
    _kindStep,
    kernel,
    nextSlot,
    previousSlot,
    priority,
    timeout,
    cancelToken,
    taskId,
  );

  Future<Object?> _submit(
    int kind,
    Function body,
    int slotA,
    int slotB,
    AetherPriority priority,
    Duration? timeout,
    AetherCancelToken? cancelToken,
    int? taskId, [
    void Function(double fraction)? onProgress,
  ]) {
    final id = taskId ?? _nextTaskId++;
    if (_shuttingDown) {
      return Future<Object?>.error(AetherPoolShutdown(id));
    }
    if ((kind == _kindGrid || kind == _kindStep) && _buffer == null) {
      return Future<Object?>.error(
        StateError('This pool was started without a memory buffer'),
      );
    }
    if (cancelToken != null && cancelToken.isCancelled) {
      return Future<Object?>.error(AetherTaskCancelled(id));
    }
    final task = _WebTask(
      id,
      kind,
      body,
      slotA,
      slotB,
      priority.index,
      timeout ?? config.taskTimeout,
      _clock.elapsedMicroseconds,
      cancelToken,
      config.enableTimeline ? (TimelineTask()..start('aether.task')) : null,
    )..onProgress = onProgress;
    if (cancelToken != null) {
      void listener(bool hard) => _onCancel(task, hard);
      task.cancelListener = listener;
      cancelToken.addListener(listener);
    }
    _queues[task.level].addLast(task);
    _queuedCounts[task.level]++;
    _scheduleDispatch();
    return task.completer.future;
  }

  void _onCancel(_WebTask task, bool hard) {
    switch (task.state) {
      case _TaskState.queued:
        _queuedCounts[task.level]--;
        _finish(task, failure: AetherTaskCancelled(task.id));
      case _TaskState.running:
        if (hard) {
          _abandon(task, AetherTaskCancelled(task.id));
        } else {
          task.softCancelled = true;
        }
      case _TaskState.done:
        break;
    }
  }

  void _scheduleDispatch() {
    if (_dispatchScheduled || _shuttingDown) {
      return;
    }
    _dispatchScheduled = true;
    // A macrotask, not a microtask: the browser can draw a frame and handle
    // input between two tasks.
    Timer.run(_dispatch);
  }

  void _dispatch() {
    _dispatchScheduled = false;
    if (_shuttingDown || _running != null) {
      return;
    }
    final task = _nextTask();
    if (task != null) {
      _start(task);
    }
  }

  /// Same rule as the native pool: submitted level plus one per aging
  /// interval waited, capped at high; ties go to the longest waiter.
  _WebTask? _nextTask() {
    final agingMicros = config.agingInterval.inMicroseconds;
    final now = _clock.elapsedMicroseconds;
    final top = AetherPriority.high.index;
    _WebTask? best;
    var bestLevel = -1;
    var bestScore = -1;
    for (var level = top; level >= 0; level--) {
      final queue = _queues[level];
      while (queue.isNotEmpty && queue.first.state != _TaskState.queued) {
        queue.removeFirst();
      }
      if (queue.isEmpty) {
        continue;
      }
      final head = queue.first;
      final waited = now - head.submittedAtMicros;
      final score = agingMicros > 0
          ? math.min(top, level + waited ~/ agingMicros)
          : level;
      if (score > bestScore ||
          (score == bestScore &&
              head.submittedAtMicros < best!.submittedAtMicros)) {
        best = head;
        bestLevel = level;
        bestScore = score;
      }
    }
    if (best == null) {
      return null;
    }
    _queues[bestLevel].removeFirst();
    _queuedCounts[bestLevel]--;
    return best;
  }

  void _start(_WebTask task) {
    task.state = _TaskState.running;
    _running = task;
    final timeout = task.timeout;
    if (timeout != null) {
      task.deadline = AetherDeadline(
        timeout,
        () => _abandon(task, AetherTaskTimeout(task.id, timeout)),
        paused: _watchdogPaused,
      );
    }
    final Future<Object?> result = runZoned(
      () => Future<Object?>.sync(() => _invoke(task)),
      zoneValues: _markedZone(task.marker),
    );
    result.then<void>(
      (value) => _settle(task, value: value),
      onError: (Object error, StackTrace stack) =>
          _settle(task, error: error, stack: stack),
    );
  }

  FutureOr<Object?> _invoke(_WebTask task) {
    switch (task.kind) {
      case _kindValue:
        return (task.body as FutureOr<Object?> Function())();
      case _kindProgress:
        final clock = Stopwatch()..start();
        var lastSentMicros = -aetherProgressIntervalMicros;
        void report(double fraction) {
          if (task.state != _TaskState.running || task.softCancelled) {
            return;
          }
          final value = fraction.isNaN ? 0.0 : fraction.clamp(0.0, 1.0);
          final now = clock.elapsedMicroseconds;
          if (value >= 1.0 ||
              now - lastSentMicros >= aetherProgressIntervalMicros) {
            lastSentMicros = now;
            _callUser(task.onProgress, value);
          }
        }

        return (task.body as AetherProgressTask<Object?>)(report);
      case _kindGrid:
        final buffer = _buffer!;
        return (task.body as AetherGridKernel)(
          buffer.slotView(task.slotA),
          buffer.readHeader(task.slotA),
        );
      case _kindStep:
        final buffer = _buffer!;
        return (task.body as AetherStepKernel)(
          buffer.slotView(task.slotB),
          buffer.slotView(task.slotA),
          buffer.readHeader(task.slotA),
        );
      default:
        throw StateError('Unknown task kind ${task.kind}');
    }
  }

  void _settle(
    _WebTask task, {
    Object? value,
    Object? error,
    StackTrace? stack,
  }) {
    task.deadline?.cancel();
    task.marker.active = false;
    if (task.state == _TaskState.running) {
      final pending = task.pendingFailure;
      if (pending != null) {
        _finish(task, failure: pending);
      } else if (task.softCancelled) {
        _finish(task, failure: AetherTaskCancelled(task.id));
      } else if (error != null) {
        _finish(
          task,
          failure: AetherTaskException(task.id, error, stack.toString()),
        );
      } else {
        _finish(task, value: value);
      }
    }
    // An abandoned task settling late finds another task running; leave it.
    if (identical(_running, task)) {
      _running = null;
      _scheduleDispatch();
    }
  }

  /// Fails a running task now and moves on. Its code may still be running;
  /// its result will be discarded.
  ///
  /// Grid and step kernels are the exception: they write a slot, and the
  /// slot may only be handed on once nothing writes it any more. Nothing can
  /// stop running code on the web, so their failure is recorded and
  /// delivered when the kernel actually finishes, and the queue waits.
  void _abandon(_WebTask task, AetherFailure failure) {
    if (task.state != _TaskState.running) {
      return;
    }
    task.deadline?.cancel();
    if (task.kind == _kindGrid || task.kind == _kindStep) {
      task.pendingFailure ??= failure;
      return;
    }
    task.marker.active = false;
    _finish(task, failure: failure);
    if (identical(_running, task)) {
      _running = null;
      _scheduleDispatch();
    }
  }

  /// Freezes running timeouts while the app is in the background.
  void pauseWatchdog() {
    _watchdogPaused = true;
    _running?.deadline?.pause();
  }

  /// Restarts the clocks frozen by [pauseWatchdog].
  void resumeWatchdog() {
    _watchdogPaused = false;
    _running?.deadline?.resume();
  }

  /// Whether the timeout clocks are frozen.
  bool get isWatchdogPaused => _watchdogPaused;

  void _finish(_WebTask task, {Object? value, AetherFailure? failure}) {
    if (task.state == _TaskState.done) {
      return;
    }
    task.state = _TaskState.done;
    final listener = task.cancelListener;
    if (listener != null) {
      task.cancelToken!.removeListener(listener);
    }
    task.timeline?.finish();
    _latency[_latencyNext] =
        _clock.elapsedMicroseconds - task.submittedAtMicros;
    _latencyNext = (_latencyNext + 1) % _latency.length;
    if (_latencyCount < _latency.length) {
      _latencyCount++;
    }
    if (failure == null) {
      _completed++;
      task.completer.complete(value);
      return;
    }
    _failed++;
    task.completer.completeError(failure);
    if (failure is! AetherTaskCancelled && failure is! AetherPoolShutdown) {
      _callUser(onFailure, failure);
    }
  }

  void _callUser<A>(void Function(A)? callback, A argument) {
    if (callback == null) {
      return;
    }
    try {
      callback(argument);
    } catch (error, stack) {
      _zone.handleUncaughtError(error, stack);
    }
  }

  /// A snapshot of queue depths and latency percentiles.
  AetherPoolStats stats() {
    final busy = _running == null ? 0 : 1;
    final samples = List<int>.of(_latency.sublist(0, _latencyCount))..sort();
    int percentile(double p) => samples.isEmpty
        ? 0
        : samples[math.min(samples.length - 1, (samples.length * p).floor())];
    return AetherPoolStats(
      workerCount: 1,
      idleWorkers: 1 - busy,
      busyWorkers: busy,
      startingWorkers: 0,
      queuedHigh: _queuedCounts[AetherPriority.high.index],
      queuedNormal: _queuedCounts[AetherPriority.normal.index],
      queuedLow: _queuedCounts[AetherPriority.low.index],
      completedTasks: _completed,
      failedTasks: _failed,
      respawns: 0,
      latencySamples: samples.length,
      p50Micros: percentile(0.50),
      p99Micros: percentile(0.99),
      maxMicros: samples.isEmpty ? 0 : samples.last,
    );
  }

  /// Fails queued tasks and abandons the running one with
  /// [AetherPoolShutdown]. Safe to call more than once.
  Future<void> shutdown() async {
    if (_shuttingDown) {
      return;
    }
    _shuttingDown = true;
    for (var level = 0; level < _queues.length; level++) {
      final queue = _queues[level];
      while (queue.isNotEmpty) {
        final task = queue.removeFirst();
        if (task.state == _TaskState.queued) {
          _finish(task, failure: AetherPoolShutdown(task.id));
        }
      }
      _queuedCounts[level] = 0;
    }
    final running = _running;
    if (running != null) {
      _abandon(running, AetherPoolShutdown(running.id));
    }
  }
}
