// MODULE 2 — The warm isolate worker pool.
//
// A fixed set of worker isolates is spawned once and kept alive. Each worker
// attaches to the shared memory grid and then waits for tasks. The pool sends
// a worker at most one task at a time, so a worker never needs a queue of its
// own; all queueing, prioritisation, aging, timeouts and crash recovery
// happen here, on the owning isolate.
//
// This file depends only on dart: libraries, so it also runs (and is
// benchmarked) outside Flutter.

import 'dart:async';
import 'dart:collection';
import 'dart:developer' show TimelineTask;
import 'dart:isolate';
import 'dart:math' as math;
import 'dart:typed_data';

import 'aether_failures.dart';
import 'aether_memory_grid.dart';
import 'aether_pool_types.dart';

export 'aether_pool_types.dart';

// ---------------------------------------------------------------------------
// Worker isolate side.
// ---------------------------------------------------------------------------

abstract final class _WorkerContext {
  static bool isWorker = false;
}

/// Marks the current isolate as an Aether worker, so nested runner calls run
/// inline. Used by one-shot isolates; not part of the public API.
void markAetherWorkerIsolate() {
  _WorkerContext.isWorker = true;
}

const int _kindValue = 0;
const int _kindGrid = 1;
const int _kindStep = 2;
const int _kindProgress = 3;

const int _statusOk = 0;
const int _statusThrew = 1;
const int _statusUnsendableResult = 2;

final class _WorkerBoot {
  const _WorkerBoot(
    this.index,
    this.generation,
    this.replyPort,
    this.buffer,
    this.init,
  );

  final int index;
  final int generation;
  final SendPort replyPort;
  final AetherBufferDescriptor? buffer;
  final AetherWorkerInit? init;
}

final class _WorkMessage {
  const _WorkMessage(this.taskId, this.kind, this.body, this.slotA, this.slotB);

  final int taskId;
  final int kind;
  final Function body;
  final int slotA;
  final int slotB;
}

final class _Ready {
  const _Ready(this.index, this.generation, this.port);

  final int index;
  final int generation;
  final SendPort port;
}

final class _InitFailed {
  const _InitFailed(this.index, this.generation, this.detail);

  final int index;
  final int generation;
  final String detail;
}

final class _Progress {
  const _Progress(this.index, this.generation, this.taskId, this.value);

  final int index;
  final int generation;
  final int taskId;
  final double value;
}

final class _Reply {
  const _Reply(
    this.index,
    this.generation,
    this.taskId,
    this.status,
    this.value,
    this.stack,
  );

  final int index;
  final int generation;
  final int taskId;
  final int status;
  final Object? value;
  final String stack;
}

Future<void> _workerMain(_WorkerBoot boot) async {
  _WorkerContext.isWorker = true;
  final descriptor = boot.buffer;
  final buffer = descriptor == null
      ? null
      : ContiguousMemoryBuffer.attach(descriptor);
  try {
    await boot.init?.call();
  } catch (error, stack) {
    boot.replyPort.send(
      _InitFailed(boot.index, boot.generation, '$error\n$stack'),
    );
    return;
  }
  final port = RawReceivePort();
  port.handler = (Object? message) {
    unawaited(_execute(boot, buffer, message! as _WorkMessage));
  };
  boot.replyPort.send(_Ready(boot.index, boot.generation, port.sendPort));
}

Future<void> _execute(
  _WorkerBoot boot,
  ContiguousMemoryBuffer? buffer,
  _WorkMessage work,
) async {
  Object? value;
  try {
    switch (work.kind) {
      case _kindValue:
        value = await (work.body as FutureOr<Object?> Function())();
      case _kindGrid:
        final grid = _requireBuffer(buffer);
        await (work.body as AetherGridKernel)(
          grid.slotView(work.slotA),
          grid.readHeader(work.slotA),
        );
      case _kindProgress:
        final clock = Stopwatch()..start();
        var lastSentMicros = -aetherProgressIntervalMicros;
        void report(double fraction) {
          final value = fraction.isNaN ? 0.0 : fraction.clamp(0.0, 1.0);
          final now = clock.elapsedMicroseconds;
          if (value >= 1.0 ||
              now - lastSentMicros >= aetherProgressIntervalMicros) {
            lastSentMicros = now;
            boot.replyPort.send(
              _Progress(boot.index, boot.generation, work.taskId, value),
            );
          }
        }

        value = await (work.body as AetherProgressTask<Object?>)(report);
      case _kindStep:
        final grid = _requireBuffer(buffer);
        await (work.body as AetherStepKernel)(
          grid.slotView(work.slotB),
          grid.slotView(work.slotA),
          grid.readHeader(work.slotA),
        );
      default:
        throw StateError('Unknown task kind ${work.kind}');
    }
  } catch (error, stack) {
    _reply(boot, work.taskId, _statusThrew, error, stack.toString());
    return;
  }
  _reply(boot, work.taskId, _statusOk, value, '');
}

ContiguousMemoryBuffer _requireBuffer(ContiguousMemoryBuffer? buffer) {
  if (buffer == null) {
    throw StateError('This pool was started without a memory buffer');
  }
  return buffer;
}

void _reply(
  _WorkerBoot boot,
  int taskId,
  int status,
  Object? value,
  String stack,
) {
  try {
    boot.replyPort.send(
      _Reply(boot.index, boot.generation, taskId, status, value, stack),
    );
  } on ArgumentError catch (sendError) {
    // The value (a result or a thrown error) cannot cross isolates. A thrown
    // error is still reported, as text; an unsendable result is its own
    // failure.
    if (status == _statusThrew) {
      boot.replyPort.send(
        _Reply(
          boot.index,
          boot.generation,
          taskId,
          _statusThrew,
          value.toString(),
          stack,
        ),
      );
    } else {
      boot.replyPort.send(
        _Reply(
          boot.index,
          boot.generation,
          taskId,
          _statusUnsendableResult,
          describeUnsendable(sendError, 'result'),
          '',
        ),
      );
    }
  }
}

// ---------------------------------------------------------------------------
// Owning isolate side.
// ---------------------------------------------------------------------------

enum _WorkerState { dead, starting, idle, busy, stopping }

enum _KillReason { none, timeout, hardCancel, shutdown, initFailure }

enum _TaskState { queued, running, done }

final class _Worker {
  _Worker(this.index);

  final int index;
  int generation = 0;
  _WorkerState state = _WorkerState.dead;
  _KillReason killReason = _KillReason.none;
  Isolate? isolate;
  SendPort? port;
  RawReceivePort? exitPort;
  RawReceivePort? errorPort;
  _PoolTask? task;
  String? lastError;
  int startFailures = 0;
  final Stopwatch watch = Stopwatch();
  Completer<void>? ready;
  Completer<void>? exited;
  Future<void>? spawning;
  Timer? respawnTimer;
}

final class _PoolTask {
  _PoolTask(
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
  _TaskState state = _TaskState.queued;
  _Worker? worker;
  bool softCancelled = false;
}

/// A fixed pool of warm worker isolates with priority queues, aging,
/// per-task timeouts, crash recovery and cancellation.
///
/// Most apps use `AetherConcurrentRunner` instead of this class directly.
final class AetherWorkerPool {
  AetherWorkerPool._(this.config, this._buffer, this._workerInit)
    : _workers = List<_Worker>.generate(
        config.resolvedWorkerCount,
        _Worker.new,
        growable: false,
      ),
      _latency = Int64List(math.max(1, config.latencySampleCapacity));

  /// Spawns every worker and completes once all of them are ready.
  ///
  /// When [buffer] is given, workers attach to it so grid and step tasks can
  /// run. [workerInit] runs on each worker before it takes work. Throws
  /// [AetherWorkerInitFailure] (after cleaning up) if any worker fails to
  /// start.
  static Future<AetherWorkerPool> start({
    AetherPoolConfig config = const AetherPoolConfig(),
    ContiguousMemoryBuffer? buffer,
    AetherWorkerInit? workerInit,
  }) async {
    if (config.resolvedWorkerCount < 1) {
      throw RangeError.range(config.resolvedWorkerCount, 1, null, 'workers');
    }
    final pool = AetherWorkerPool._(config, buffer, workerInit);
    await pool._startAll();
    return pool;
  }

  /// Whether the current isolate is an Aether worker.
  static bool get isWorkerIsolate => _WorkerContext.isWorker;

  /// The configuration this pool was started with.
  final AetherPoolConfig config;

  /// Called for every failure except [AetherTaskCancelled] and
  /// [AetherPoolShutdown], which the caller caused.
  void Function(AetherFailure failure)? onFailure;

  // Held (not just its address) so the buffer's finalizer cannot free the
  // memory while workers use it.
  final ContiguousMemoryBuffer? _buffer;
  final AetherWorkerInit? _workerInit;
  final List<_Worker> _workers;
  final RawReceivePort _replyPort = RawReceivePort();
  final Zone _zone = Zone.current;
  final Stopwatch _clock = Stopwatch()..start();
  final List<ListQueue<_PoolTask>> _queues =
      List<ListQueue<_PoolTask>>.generate(
        AetherPriority.values.length,
        (_) => ListQueue<_PoolTask>(64),
        growable: false,
      );
  final Int32List _queuedCounts = Int32List(AetherPriority.values.length);
  final Int64List _latency;
  int _latencyCount = 0;
  int _latencyNext = 0;
  int _nextTaskId = 1;
  int _completed = 0;
  int _failed = 0;
  int _respawns = 0;
  bool _started = false;
  bool _shuttingDown = false;
  bool _watchdogPaused = false;
  bool _dispatchScheduled = false;
  Timer? _watchdog;
  Timer? _cleanup;
  Future<void>? _shutdown;

  /// Whether [shutdown] has been called.
  bool get isShutDown => _shuttingDown;

  /// Number of workers.
  int get workerCount => _workers.length;

  Future<void> _startAll() async {
    _replyPort.handler = _onReply;
    for (final worker in _workers) {
      worker.spawning = _spawn(worker);
    }
    final readies = <Future<void>>[
      for (final worker in _workers) worker.ready!.future,
    ];
    try {
      await Future.wait(readies, eagerError: true);
    } on Object catch (error) {
      // Let every spawn settle so shutdown sees every isolate.
      for (final ready in readies) {
        unawaited(ready.then<void>((_) {}, onError: (Object _) {}));
      }
      await shutdown();
      if (error is AetherWorkerInitFailure) {
        rethrow;
      }
      throw AetherWorkerInitFailure(-1, '$error');
    }
    _started = true;
    _cleanup = Timer.periodic(config.cleanupInterval, (_) => _runCleanup());
  }

  Future<void> _spawn(_Worker worker) async {
    worker.generation++;
    final generation = worker.generation;
    worker
      ..state = _WorkerState.starting
      ..killReason = _KillReason.none
      ..lastError = null
      ..port = null
      ..ready = Completer<void>()
      ..exited = Completer<void>();
    // Only start-up awaits `ready`; a respawn failing must not surface as an
    // unhandled error.
    worker.ready!.future.ignore();
    final exitPort = RawReceivePort();
    final errorPort = RawReceivePort();
    exitPort.handler = (Object? _) => _onExit(worker, generation);
    errorPort.handler = (Object? message) {
      if (message is List<Object?> && message.isNotEmpty) {
        worker.lastError = message.length > 1
            ? '${message[0]}\n${message[1]}'
            : '${message[0]}';
      }
    };
    worker
      ..exitPort = exitPort
      ..errorPort = errorPort;
    try {
      worker.isolate = await Isolate.spawn<_WorkerBoot>(
        _workerMain,
        _WorkerBoot(
          worker.index,
          generation,
          _replyPort.sendPort,
          _buffer?.descriptor,
          _workerInit,
        ),
        onExit: exitPort.sendPort,
        onError: errorPort.sendPort,
        errorsAreFatal: true,
        debugName: 'aether-worker-${worker.index}',
      );
    } on Object catch (error) {
      // Spawn itself failed (for example an unsendable workerInit). No
      // isolate exists, so no exit will arrive: clean up here.
      exitPort.close();
      errorPort.close();
      worker
        ..exitPort = null
        ..errorPort = null
        ..state = _WorkerState.dead;
      if (!worker.ready!.isCompleted) {
        worker.ready!.completeError(
          AetherWorkerInitFailure(worker.index, 'Isolate.spawn failed: $error'),
        );
      }
      worker.exited!.complete();
      _scheduleRespawn(worker, startupFailure: true);
      return;
    }
    if (_shuttingDown && worker.generation == generation) {
      if (worker.killReason == _KillReason.none) {
        worker.killReason = _KillReason.shutdown;
      }
      worker.isolate!.kill(priority: Isolate.immediate);
    }
  }

  void _onReply(Object? message) {
    switch (message) {
      case _Reply():
        _onTaskReply(message);
      case _Progress():
        final worker = _workers[message.index];
        final task = worker.task;
        if (worker.generation == message.generation &&
            task != null &&
            task.id == message.taskId &&
            !task.softCancelled) {
          _callUser(task.onProgress, message.value);
        }
      case _Ready():
        final worker = _workers[message.index];
        if (worker.generation != message.generation ||
            worker.state != _WorkerState.starting) {
          return;
        }
        worker
          ..port = message.port
          ..state = _WorkerState.idle
          ..startFailures = 0;
        worker.ready!.complete();
        _scheduleDispatch();
      case _InitFailed():
        final worker = _workers[message.index];
        if (worker.generation != message.generation) {
          return;
        }
        worker
          ..lastError = message.detail
          ..killReason = _KillReason.initFailure
          ..state = _WorkerState.stopping;
        if (!worker.ready!.isCompleted) {
          worker.ready!.completeError(
            AetherWorkerInitFailure(
              worker.index,
              'workerInit failed: ${message.detail}',
            ),
          );
        }
        worker.isolate?.kill(priority: Isolate.immediate);
    }
  }

  void _onTaskReply(_Reply reply) {
    final worker = _workers[reply.index];
    if (worker.generation != reply.generation) {
      return;
    }
    final task = worker.task;
    if (task == null || task.id != reply.taskId) {
      return;
    }
    worker.watch
      ..stop()
      ..reset();
    worker.task = null;
    if (worker.state == _WorkerState.busy) {
      worker.state = _WorkerState.idle;
    }
    // If a kill is already on its way (a timeout or hard cancel raced this
    // reply), the worker has still let go of the slot, so the result stands
    // and the exit that follows only respawns the worker.
    if (task.softCancelled) {
      _finish(task, failure: AetherTaskCancelled(task.id));
    } else if (reply.status == _statusOk) {
      _finish(task, value: reply.value);
    } else if (reply.status == _statusThrew) {
      _finish(
        task,
        failure: AetherTaskException(task.id, reply.value!, reply.stack),
      );
    } else {
      _finish(task, failure: AetherUnsendableResult(task.id, '${reply.value}'));
    }
    _scheduleDispatch();
  }

  void _onExit(_Worker worker, int generation) {
    if (worker.generation != generation) {
      return;
    }
    worker.exitPort?.close();
    worker.errorPort?.close();
    final wasStarting =
        worker.state == _WorkerState.starting ||
        worker.killReason == _KillReason.initFailure;
    final reason = worker.killReason;
    final task = worker.task;
    worker
      ..exitPort = null
      ..errorPort = null
      ..isolate = null
      ..port = null
      ..task = null
      ..state = _WorkerState.dead;
    worker.watch
      ..stop()
      ..reset();
    if (!worker.ready!.isCompleted) {
      worker.ready!.completeError(
        AetherWorkerInitFailure(
          worker.index,
          worker.lastError ?? 'Worker exited during start-up',
        ),
      );
    }
    worker.exited!.complete();
    if (task != null && task.state == _TaskState.running) {
      final AetherFailure failure = switch (reason) {
        _KillReason.timeout => AetherTaskTimeout(task.id, task.timeout!),
        _KillReason.hardCancel => AetherTaskCancelled(task.id),
        _KillReason.shutdown => AetherPoolShutdown(task.id),
        _ => AetherWorkerCrash(
          task.id,
          worker.index,
          worker.lastError ?? 'Worker isolate exited while running the task',
        ),
      };
      _finish(task, failure: failure);
    }
    if (!_shuttingDown && _started) {
      _respawns++;
      _scheduleRespawn(worker, startupFailure: wasStarting);
    }
  }

  static const int _maxStartFailures = 3;

  /// True when no worker is alive and each has failed to start
  /// [_maxStartFailures] times in a row.
  bool get _allWorkersFailing => _workers.every(
    (worker) =>
        (worker.state == _WorkerState.dead ||
            worker.state == _WorkerState.starting) &&
        worker.startFailures >= _maxStartFailures,
  );

  void _failQueued(AetherFailure Function(int taskId) failureFor) {
    for (var level = 0; level < _queues.length; level++) {
      final queue = _queues[level];
      while (queue.isNotEmpty) {
        final task = queue.removeFirst();
        if (task.state == _TaskState.queued) {
          _finish(task, failure: failureFor(task.id));
        }
      }
      _queuedCounts[level] = 0;
    }
  }

  void _scheduleRespawn(_Worker worker, {required bool startupFailure}) {
    if (_shuttingDown || !_started) {
      return;
    }
    worker.respawnTimer?.cancel();
    if (!startupFailure) {
      worker.startFailures = 0;
      worker.spawning = _spawn(worker);
      return;
    }
    worker.startFailures++;
    if (_allWorkersFailing) {
      // Nothing can run: fail what is waiting instead of letting it hang.
      // Respawning continues below, so the pool recovers when a worker can
      // start again.
      _failQueued(
        (id) => AetherWorkerInitFailure(
          worker.index,
          'Every worker failed to start $_maxStartFailures times in a row; '
          'last error: ${worker.lastError ?? 'unknown'}',
        ),
      );
    }
    final delayMs = math.min(
      5000,
      100 * math.pow(2, worker.startFailures - 1).toInt(),
    );
    worker.respawnTimer = Timer(Duration(milliseconds: delayMs), () {
      worker.respawnTimer = null;
      if (!_shuttingDown && worker.state == _WorkerState.dead) {
        worker.spawning = _spawn(worker);
      }
    });
  }

  /// Reserves a task id, so a caller can write it into a payload header
  /// before submitting.
  int reserveTaskId() => _nextTaskId++;

  /// Runs [task] on a worker and completes with its result.
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

  /// Like [run], for a task that reports progress. [onProgress] runs on
  /// this isolate as reports arrive (throttled to about 60 per second).
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

  /// Runs [kernel] over [slot] of the shared buffer.
  ///
  /// The returned future completes only once the worker no longer touches
  /// the slot, on success and on every failure.
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
    if (_started && _allWorkersFailing) {
      final failure = AetherWorkerInitFailure(
        -1,
        'No worker can start (each failed $_maxStartFailures times in a '
        'row); the pool keeps retrying in the background',
      );
      _callUser(onFailure, failure);
      return Future<Object?>.error(failure);
    }
    if ((kind == _kindGrid || kind == _kindStep) && _buffer == null) {
      return Future<Object?>.error(
        StateError('This pool was started without a memory buffer'),
      );
    }
    if (cancelToken != null && cancelToken.isCancelled) {
      return Future<Object?>.error(AetherTaskCancelled(id));
    }
    final task = _PoolTask(
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
    );
    task.onProgress = onProgress;
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

  void _onCancel(_PoolTask task, bool hard) {
    switch (task.state) {
      case _TaskState.queued:
        _queuedCounts[task.level]--;
        _finish(task, failure: AetherTaskCancelled(task.id));
      case _TaskState.running:
        final worker = task.worker!;
        if (hard && worker.task == task && worker.isolate != null) {
          // A timeout already on its way keeps its reason.
          if (worker.killReason == _KillReason.none) {
            worker.killReason = _KillReason.hardCancel;
          }
          worker.state = _WorkerState.stopping;
          worker.isolate!.kill(priority: Isolate.immediate);
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
    scheduleMicrotask(_dispatch);
  }

  void _dispatch() {
    _dispatchScheduled = false;
    if (_shuttingDown) {
      return;
    }
    for (final worker in _workers) {
      if (worker.state != _WorkerState.idle) {
        continue;
      }
      final task = _nextTask();
      if (task == null) {
        return;
      }
      _start(worker, task);
    }
  }

  /// Takes the task with the highest effective priority.
  ///
  /// Effective priority is the submitted level plus one for every
  /// [AetherPoolConfig.agingInterval] the task has waited, capped at high.
  /// Each queue is FIFO, so only the three queue heads need comparing; ties
  /// go to the task that has waited longest. A low task therefore outranks
  /// fresh high work after at most two aging intervals.
  _PoolTask? _nextTask() {
    final agingMicros = config.agingInterval.inMicroseconds;
    final now = _clock.elapsedMicroseconds;
    final top = AetherPriority.high.index;
    _PoolTask? best;
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

  void _start(_Worker worker, _PoolTask task) {
    task
      ..state = _TaskState.running
      ..worker = worker;
    worker
      ..task = task
      ..state = _WorkerState.busy;
    worker.watch.reset();
    if (!_watchdogPaused) {
      worker.watch.start();
    }
    try {
      worker.port!.send(
        _WorkMessage(task.id, task.kind, task.body, task.slotA, task.slotB),
      );
    } on ArgumentError catch (error) {
      // Nothing reached the worker, so it is still idle and healthy.
      worker
        ..task = null
        ..state = _WorkerState.idle;
      worker.watch
        ..stop()
        ..reset();
      _finish(
        task,
        failure: AetherUnsendableTask(
          task.id,
          describeUnsendable(error, 'task closure'),
        ),
      );
      _scheduleDispatch();
      return;
    }
    if (task.timeout != null) {
      _watchdog ??= Timer.periodic(config.watchdogTick, (_) => _onWatchdog());
    }
  }

  void _onWatchdog() {
    var anyTimed = false;
    for (final worker in _workers) {
      final task = worker.task;
      if (worker.state != _WorkerState.busy || task == null) {
        continue;
      }
      final timeout = task.timeout;
      if (timeout == null) {
        continue;
      }
      anyTimed = true;
      if (worker.watch.elapsedMicroseconds > timeout.inMicroseconds) {
        worker
          ..killReason = _KillReason.timeout
          ..state = _WorkerState.stopping;
        worker.isolate?.kill(priority: Isolate.immediate);
      }
    }
    if (!anyTimed) {
      _watchdog?.cancel();
      _watchdog = null;
    }
  }

  /// Freezes every running task's timeout clock, for example while the app
  /// is in the background and the OS has suspended the workers.
  void pauseWatchdog() {
    _watchdogPaused = true;
    for (final worker in _workers) {
      worker.watch.stop();
    }
  }

  /// Restarts the timeout clocks frozen by [pauseWatchdog].
  void resumeWatchdog() {
    _watchdogPaused = false;
    for (final worker in _workers) {
      if (worker.state == _WorkerState.busy) {
        worker.watch.start();
      }
    }
  }

  /// Whether the timeout clocks are frozen.
  bool get isWatchdogPaused => _watchdogPaused;

  void _finish(_PoolTask task, {Object? value, AetherFailure? failure}) {
    if (task.state == _TaskState.done) {
      return;
    }
    task.state = _TaskState.done;
    final listener = task.cancelListener;
    if (listener != null) {
      task.cancelToken!.removeListener(listener);
    }
    task.timeline?.finish();
    final elapsed = _clock.elapsedMicroseconds - task.submittedAtMicros;
    _latency[_latencyNext] = elapsed;
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

  /// Calls a user callback without letting it throw into pool bookkeeping.
  ///
  /// Port handlers run in the root zone, so a throw is reported to the zone
  /// the pool was started in (the app's or test's error zone), not the root.
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

  void _runCleanup() {
    if (_shuttingDown) {
      return;
    }
    for (var level = 0; level < _queues.length; level++) {
      final queue = _queues[level];
      if (queue.length != _queuedCounts[level]) {
        queue.removeWhere((task) => task.state != _TaskState.queued);
      }
    }
    for (final worker in _workers) {
      if (worker.state == _WorkerState.dead && worker.respawnTimer == null) {
        worker.spawning = _spawn(worker);
      }
    }
    _scheduleDispatch();
  }

  /// A snapshot of worker states, queue depths and latency percentiles.
  AetherPoolStats stats() {
    var idle = 0;
    var busy = 0;
    var starting = 0;
    for (final worker in _workers) {
      switch (worker.state) {
        case _WorkerState.idle:
          idle++;
        case _WorkerState.busy:
        case _WorkerState.stopping:
          busy++;
        case _WorkerState.starting:
        case _WorkerState.dead:
          starting++;
      }
    }
    final samples = Int64List.fromList(_latency.sublist(0, _latencyCount))
      ..sort();
    int percentile(double p) => samples.isEmpty
        ? 0
        : samples[math.min(samples.length - 1, (samples.length * p).floor())];
    return AetherPoolStats(
      workerCount: _workers.length,
      idleWorkers: idle,
      busyWorkers: busy,
      startingWorkers: starting,
      queuedHigh: _queuedCounts[AetherPriority.high.index],
      queuedNormal: _queuedCounts[AetherPriority.normal.index],
      queuedLow: _queuedCounts[AetherPriority.low.index],
      completedTasks: _completed,
      failedTasks: _failed,
      respawns: _respawns,
      latencySamples: samples.length,
      p50Micros: percentile(0.50),
      p99Micros: percentile(0.99),
      maxMicros: samples.isEmpty ? 0 : samples.last,
    );
  }

  /// Stops the pool: fails queued tasks with [AetherPoolShutdown], kills
  /// every worker, waits for each exit, fails in-flight tasks, then closes
  /// the reply port. Safe to call more than once.
  Future<void> shutdown() => _shutdown ??= _doShutdown();

  Future<void> _doShutdown() async {
    _shuttingDown = true;
    _watchdog?.cancel();
    _watchdog = null;
    _cleanup?.cancel();
    _cleanup = null;
    _failQueued(AetherPoolShutdown.new);
    for (final worker in _workers) {
      worker.respawnTimer?.cancel();
      worker.respawnTimer = null;
    }
    // Let in-progress spawns resolve so every isolate is known.
    await Future.wait<void>(<Future<void>>[
      for (final worker in _workers)
        if (worker.spawning != null) worker.spawning!,
    ]);
    final exits = <Future<void>>[];
    for (final worker in _workers) {
      final isolate = worker.isolate;
      if (isolate != null) {
        if (worker.killReason == _KillReason.none) {
          worker.killReason = _KillReason.shutdown;
        }
        worker.state = _WorkerState.stopping;
        exits.add(worker.exited!.future);
        isolate.kill(priority: Isolate.immediate);
      }
    }
    await Future.wait(exits);
    _replyPort.close();
  }
}
