// MODULE 4 — The concurrency interface apps call.
//
// AetherConcurrentRunner owns one memory grid, one worker pool and one state
// stitcher for the app's lifetime. Apps call computeAsync / computeGrid /
// startContinuous and never see an isolate or a port.

import 'dart:async';
import 'dart:collection';
import 'dart:math' as math;

import 'package:flutter/foundation.dart';
import 'package:flutter/widgets.dart';

import 'aether_failures.dart';
import 'aether_platform.dart';
import 'aether_state_stitcher.dart';

/// Called on the UI isolate with a slot's data.
typedef AetherSlotCallback =
    void Function(Float32List slot, AetherPayload payload);

/// Runtime settings. Every field has a working default.
final class AetherConfig {
  /// Creates a runtime configuration.
  const AetherConfig({
    this.pool = const AetherPoolConfig(),
    this.slotCount = 8,
    this.slotStrideFloats = 65536,
    this.nodeStride = 4,
    this.maxPendingGrid = 64,
    this.maxPendingFrameUpdates = 64,
  });

  /// Worker pool settings.
  final AetherPoolConfig pool;

  /// Number of shared memory slots.
  final int slotCount;

  /// Minimum floats per slot (rounded up to a 64-byte multiple).
  final int slotStrideFloats;

  /// Floats per node.
  final int nodeStride;

  /// Grid requests allowed to wait for a free slot before
  /// [AetherCapacityExceeded].
  final int maxPendingGrid;

  /// Updates held between two frames. Must exceed [slotCount].
  final int maxPendingFrameUpdates;

  @override
  bool operator ==(Object other) =>
      other is AetherConfig &&
      other.pool == pool &&
      other.slotCount == slotCount &&
      other.slotStrideFloats == slotStrideFloats &&
      other.nodeStride == nodeStride &&
      other.maxPendingGrid == maxPendingGrid &&
      other.maxPendingFrameUpdates == maxPendingFrameUpdates;

  @override
  int get hashCode => Object.hash(
    pool,
    slotCount,
    slotStrideFloats,
    nodeStride,
    maxPendingGrid,
    maxPendingFrameUpdates,
  );
}

/// Pool, memory and frame statistics.
final class AetherStats {
  /// Creates a statistics snapshot.
  const AetherStats({
    required this.pool,
    required this.slotCount,
    required this.freeSlots,
    required this.slotWaiters,
    required this.activeContinuousGrids,
    required this.oneShotRunning,
    required this.framesApplied,
    required this.updatesApplied,
    required this.updatesCoalesced,
    required this.updatesDropped,
  });

  /// Worker and latency figures.
  final AetherPoolStats pool;

  /// Total memory slots.
  final int slotCount;

  /// Slots not handed out.
  final int freeSlots;

  /// Grid requests waiting for a slot.
  final int slotWaiters;

  /// Continuous grids currently running or paused.
  final int activeContinuousGrids;

  /// One-shot isolates currently running.
  final int oneShotRunning;

  /// Frames in which results were applied.
  final int framesApplied;

  /// Results applied in frames.
  final int updatesApplied;

  /// Results replaced by newer ones before a frame.
  final int updatesCoalesced;

  /// Failure notices dropped because too many piled up between frames.
  final int updatesDropped;

  @override
  String toString() =>
      'AetherStats($pool; slots $freeSlots/$slotCount free, '
      '$slotWaiters waiting; continuous $activeContinuousGrids, '
      'one-shot $oneShotRunning; '
      'frames $framesApplied, applied $updatesApplied, '
      'coalesced $updatesCoalesced, dropped $updatesDropped)';
}

final class _SlotWaiter {
  _SlotWaiter(this.need, this.cancelToken);

  /// Slots wanted at once: 1 for a grid task, 2 for a continuous grid.
  final int need;
  final AetherCancelToken? cancelToken;
  final Completer<List<int>> completer = Completer<List<int>>();
  void Function(bool)? listener;
}

final class _LifecycleObserver with WidgetsBindingObserver {
  _LifecycleObserver(this._runtime, this._onDetached);

  final AetherRuntime _runtime;
  final Future<void> Function() _onDetached;

  @override
  void didChangeAppLifecycleState(AppLifecycleState state) {
    switch (state) {
      case AppLifecycleState.paused:
      case AppLifecycleState.hidden:
        _runtime._pool.pauseWatchdog();
        _runtime._oneShot.pauseTimeouts();
        _runtime._stitcher.flushWithoutFrame();
      case AppLifecycleState.resumed:
        _runtime._pool.resumeWatchdog();
        _runtime._oneShot.resumeTimeouts();
      case AppLifecycleState.inactive:
        break;
      case AppLifecycleState.detached:
        unawaited(_onDetached());
    }
  }
}

/// Builds the task for one chunk of [AetherRuntime.computeMap]. Top-level,
/// so the closure captures only the chunk and the transform.
List<R> Function() _mapChunk<T, R>(List<T> chunk, R Function(T) transform) =>
    () => <R>[for (final item in chunk) transform(item)];

/// One self-contained Aether instance: a memory grid, a warm worker pool, a
/// one-shot runner and a state stitcher.
///
/// Most apps use the static [AetherConcurrentRunner], which manages one
/// default runtime. Create runtimes directly to run several independently
/// configured pools, or to inject one into code under test.
final class AetherRuntime {
  AetherRuntime._(
    this.config,
    this._buffer,
    this._pool,
    this._stitcher,
    this._onFailure,
  );

  /// Allocates the memory grid and starts the worker pool.
  ///
  /// See [AetherConcurrentRunner.initialize] for [workerInit] and
  /// [onFailure]. With [observeLifecycle] (the default) the runtime pauses
  /// its watchdog in the background and disposes itself on
  /// [AppLifecycleState.detached]. Throws [AetherWorkerInitFailure] if a
  /// worker cannot start; nothing is left allocated in that case.
  static Future<AetherRuntime> start({
    AetherConfig config = const AetherConfig(),
    AetherWorkerInit? workerInit,
    void Function(AetherFailure failure)? onFailure,
    bool observeLifecycle = true,
    @visibleForTesting AetherFrameScheduler? frameScheduler,
  }) => _start(
    config: config,
    workerInit: workerInit,
    onFailure: onFailure,
    observeLifecycle: observeLifecycle && frameScheduler == null,
    frameScheduler: frameScheduler,
    onDetached: null,
  );

  static Future<AetherRuntime> _start({
    required AetherConfig config,
    required AetherWorkerInit? workerInit,
    required void Function(AetherFailure failure)? onFailure,
    required bool observeLifecycle,
    required AetherFrameScheduler? frameScheduler,
    required Future<void> Function()? onDetached,
  }) async {
    if (AetherWorkerPool.isWorkerIsolate) {
      throw StateError('Aether runtimes must be started on the main isolate');
    }
    if (config.maxPendingFrameUpdates <= config.slotCount) {
      throw ArgumentError.value(
        config.maxPendingFrameUpdates,
        'maxPendingFrameUpdates',
        'must be greater than slotCount (${config.slotCount})',
      );
    }
    if (config.maxPendingGrid < 0) {
      throw RangeError.range(config.maxPendingGrid, 0, null, 'maxPendingGrid');
    }
    if (frameScheduler == null) {
      WidgetsFlutterBinding.ensureInitialized();
    }
    final buffer = ContiguousMemoryBuffer.allocate(
      slotCount: config.slotCount,
      slotStrideFloats: config.slotStrideFloats,
      nodeStride: config.nodeStride,
    );
    final AetherWorkerPool pool;
    try {
      pool = await AetherWorkerPool.start(
        config: config.pool,
        buffer: buffer,
        workerInit: workerInit,
      );
    } on Object {
      buffer.dispose();
      rethrow;
    }
    final runtime = AetherRuntime._(
      config,
      buffer,
      pool,
      AetherStateStitcher(
        keySpace: config.slotCount,
        maxPending: config.maxPendingFrameUpdates,
        scheduler: frameScheduler,
      ),
      onFailure,
    );
    pool.onFailure = runtime._routeFailure;
    if (observeLifecycle) {
      final observer = _LifecycleObserver(
        runtime,
        onDetached ?? runtime.dispose,
      );
      WidgetsBinding.instance.addObserver(observer);
      runtime._observer = observer;
    }
    return runtime;
  }

  /// The configuration this runtime was started with.
  final AetherConfig config;

  final ContiguousMemoryBuffer _buffer;
  final AetherWorkerPool _pool;
  final AetherStateStitcher _stitcher;
  final AetherOneShotRunner _oneShot = AetherOneShotRunner();
  final ListQueue<_SlotWaiter> _slotWaiters = ListQueue<_SlotWaiter>();
  final Set<AetherContinuousGrid> _continuous = <AetherContinuousGrid>{};
  void Function(AetherFailure failure)? _onFailure;
  _LifecycleObserver? _observer;
  Future<void>? _disposeFuture;

  /// Whether [dispose] has been called.
  bool get isDisposed => _disposeFuture != null;

  /// Called on the UI isolate, in a frame, for every failure except
  /// cancellations and shutdown.
  void Function(AetherFailure failure)? get onFailure => _onFailure;

  set onFailure(void Function(AetherFailure failure)? callback) =>
      _onFailure = callback;

  void _routeFailure(AetherFailure failure) {
    if (_onFailure == null || isDisposed) {
      return;
    }
    _stitcher.enqueue(
      AetherCallbackUpdate(onApply: () => _onFailure?.call(failure)),
    );
  }

  void _checkUsable() {
    if (isDisposed) {
      throw StateError('This AetherRuntime has been disposed');
    }
  }

  void _checkMainIsolate(String method) {
    if (AetherWorkerPool.isWorkerIsolate) {
      throw StateError(
        '$method() cannot run on a worker: memory slots are owned by the '
        'main isolate. Use computeAsync() for nested work.',
      );
    }
  }

  /// Runs [task] on a warm worker and completes with its result.
  ///
  /// [task] must only capture sendable values; otherwise the future fails
  /// with [AetherUnsendableTask], whose message names the offending object.
  /// Completes as soon as the result arrives (it never waits for a frame).
  /// Fails with an [AetherFailure] subtype on error, timeout, crash or
  /// cancellation.
  Future<T> computeAsync<T>(
    FutureOr<T> Function() task, {
    AetherPriority priority = AetherPriority.normal,
    Duration? timeout,
    AetherCancelToken? cancelToken,
  }) {
    if (AetherWorkerPool.isWorkerIsolate) {
      return Future<T>.sync(task);
    }
    _checkUsable();
    return _pool.run<T>(
      task,
      priority: priority,
      timeout: timeout,
      cancelToken: cancelToken,
    );
  }

  /// Runs [task] on a fresh isolate whose result is handed over without
  /// copying.
  ///
  /// Use it for one-off jobs that return a very large result (tens of
  /// megabytes): there it beats both [computeAsync], which must copy its
  /// reply, and Flutter's `compute()`, which has no timeout or cancellation.
  /// For small or frequent tasks [computeAsync] is much faster, because it
  /// does not start an isolate. [timeout] defaults to the pool's task
  /// timeout.
  Future<T> computeOnce<T>(
    FutureOr<T> Function() task, {
    Duration? timeout,
    AetherCancelToken? cancelToken,
  }) {
    if (AetherWorkerPool.isWorkerIsolate) {
      return Future<T>.sync(task);
    }
    _checkUsable();
    return _oneShot.run<T>(
      task,
      taskId: _pool.reserveTaskId(),
      timeout: timeout ?? config.pool.taskTimeout,
      cancelToken: cancelToken,
      onFailure: _routeFailure,
    );
  }

  /// Applies [transform] to every element of [items] across all workers and
  /// returns the results in the same order.
  ///
  /// The list is split into chunks of [chunkSize] (default: enough for about
  /// four chunks per worker). [transform] must be a top-level or static
  /// function, or a closure capturing only sendable values. If any chunk
  /// fails, the remaining chunks are cancelled and the first failure is
  /// thrown.
  Future<List<R>> computeMap<T, R>(
    List<T> items,
    R Function(T item) transform, {
    int? chunkSize,
    AetherPriority priority = AetherPriority.normal,
    Duration? timeout,
    AetherCancelToken? cancelToken,
  }) async {
    if (items.isEmpty) {
      return <R>[];
    }
    if (AetherWorkerPool.isWorkerIsolate) {
      return <R>[for (final item in items) transform(item)];
    }
    _checkUsable();
    if (chunkSize != null && chunkSize < 1) {
      throw RangeError.range(chunkSize, 1, null, 'chunkSize');
    }
    final size =
        chunkSize ??
        math.max(1, (items.length / (_pool.workerCount * 4)).ceil());
    final group = AetherCancelToken();
    void forward(bool hard) => group.cancel(hard: hard);
    cancelToken?.addListener(forward);
    final parts = <Future<List<R>>>[
      for (var start = 0; start < items.length; start += size)
        _pool.run<List<R>>(
          _mapChunk<T, R>(
            items.sublist(start, math.min(items.length, start + size)),
            transform,
          ),
          priority: priority,
          timeout: timeout,
          cancelToken: group,
        ),
    ];
    try {
      final results = await Future.wait(parts, eagerError: true);
      return <R>[for (final part in results) ...part];
    } on Object {
      group.cancel();
      for (final part in parts) {
        part.ignore();
      }
      rethrow;
    } finally {
      cancelToken?.removeListener(forward);
    }
  }

  /// Runs [task] on a warm worker, calling [onProgress] on the UI isolate
  /// as the task reports progress (a fraction from 0.0 to 1.0, throttled to
  /// about 60 updates per second; 1.0 always gets through).
  Future<T> computeWithProgress<T>(
    AetherProgressTask<T> task, {
    required void Function(double fraction) onProgress,
    AetherPriority priority = AetherPriority.normal,
    Duration? timeout,
    AetherCancelToken? cancelToken,
  }) {
    if (AetherWorkerPool.isWorkerIsolate) {
      return Future<T>.sync(() => task((_) {}));
    }
    _checkUsable();
    return _pool.runWithProgress<T>(
      task,
      onProgress: onProgress,
      priority: priority,
      timeout: timeout,
      cancelToken: cancelToken,
    );
  }

  /// Runs [kernel] on a worker over a shared memory slot holding
  /// [nodeCount] nodes, then applies the result in the next frame.
  ///
  /// [prepare] fills the slot on the UI isolate before dispatch. The kernel
  /// changes the slot in place on the worker; nothing is copied either way.
  /// [onApplied] runs in the next frame, before build, and is the only place
  /// the result may be read: the slot is recycled right after that frame.
  /// The returned future completes after [onApplied].
  ///
  /// When every slot is busy the call waits; once
  /// [AetherConfig.maxPendingGrid] calls are waiting it fails with
  /// [AetherCapacityExceeded].
  Future<AetherPayload> computeGrid({
    required AetherGridKernel kernel,
    required int nodeCount,
    AetherSlotCallback? prepare,
    AetherSlotCallback? onApplied,
    bool clearSlot = true,
    AetherPriority priority = AetherPriority.normal,
    Duration? timeout,
    AetherCancelToken? cancelToken,
  }) async {
    _checkMainIsolate('computeGrid');
    _checkUsable();
    _checkNodeCount(nodeCount);
    final taskId = _pool.reserveTaskId();
    final slot = (await _acquireSlots(taskId, 1, cancelToken)).single;
    final payload = AetherPayload(
      taskId: taskId,
      slotIndex: slot,
      nodeCount: nodeCount,
      nodeStride: _buffer.nodeStride,
      priority: priority,
    );
    final view = _buffer.slotView(slot);
    try {
      if (clearSlot) {
        _buffer.clearSlot(slot);
      }
      prepare?.call(view, payload);
      _buffer.writeHeader(payload);
      await _pool.runGrid(
        slot,
        kernel,
        priority: priority,
        timeout: timeout,
        cancelToken: cancelToken,
        taskId: taskId,
      );
    } on Object {
      // The pool only fails a grid task once the worker has let go of the
      // slot, so it is safe to recycle here.
      _releaseSlot(slot);
      rethrow;
    }
    final applied = Completer<AetherPayload>();
    _stitcher.enqueue(
      AetherCallbackUpdate(
        key: slot,
        onApply: () {
          try {
            onApplied?.call(view, payload);
            applied.complete(payload);
          } catch (error, stack) {
            // Reported like any frame callback error (FR-S6), and also
            // delivered to whoever awaits the future. The future is marked
            // handled so fire-and-forget callers do not see it twice.
            FlutterError.reportError(
              FlutterErrorDetails(
                exception: error,
                stack: stack,
                library: 'aether_thread',
                context: ErrorDescription(
                  'while running computeGrid onApplied',
                ),
              ),
            );
            applied.future.ignore();
            applied.completeError(error, stack);
          }
        },
        onAfterFrame: () => _releaseSlot(slot),
        onSuperseded: () {
          _releaseSlot(slot);
          if (!applied.isCompleted) {
            applied.completeError(AetherPoolShutdown(taskId));
          }
        },
      ),
    );
    return applied.future;
  }

  /// Starts a double-buffered simulation that advances one step per frame.
  ///
  /// Two slots are reserved. [seed] writes the initial state on the UI
  /// isolate. Each step, [kernel] runs on a worker reading the front slot
  /// (`previous`) and writing the back slot (`next`), while the UI keeps
  /// reading the front slot. The slots swap at the start of the next frame
  /// and [AetherContinuousGrid.notifier] fires. Neither side waits for the
  /// other.
  Future<AetherContinuousGrid> startContinuous({
    required AetherStepKernel kernel,
    required int nodeCount,
    AetherSlotCallback? seed,
    AetherPriority priority = AetherPriority.high,
    Duration? stepTimeout,
    AetherCancelToken? cancelToken,
  }) async {
    _checkMainIsolate('startContinuous');
    _checkUsable();
    _checkNodeCount(nodeCount);
    final firstId = _pool.reserveTaskId();
    if (_buffer.slotCount < 2) {
      throw StateError('startContinuous() needs at least 2 slots');
    }
    // Both slots are reserved together: taking them one at a time lets
    // concurrent calls each hold one slot and wait forever for a second.
    final pair = await _acquireSlots(firstId, 2, cancelToken);
    final front = pair[0];
    final back = pair[1];
    final payload = AetherPayload(
      taskId: firstId,
      slotIndex: front,
      nodeCount: nodeCount,
      nodeStride: _buffer.nodeStride,
      priority: priority,
    );
    _buffer
      ..clearSlot(front)
      ..clearSlot(back);
    try {
      seed?.call(_buffer.slotView(front), payload);
    } on Object {
      _releaseSlot(front);
      _releaseSlot(back);
      rethrow;
    }
    _buffer.writeHeader(payload);
    final grid = AetherContinuousGrid._(
      runtime: this,
      kernel: kernel,
      nodeCount: nodeCount,
      priority: priority,
      stepTimeout: stepTimeout,
      front: front,
      back: back,
      notifier: AetherSlotNotifier(_buffer.slotView(front), payload),
    );
    _continuous.add(grid);
    grid._submitStep();
    return grid;
  }

  void _checkNodeCount(int nodeCount) {
    if (nodeCount < 1 || nodeCount > _buffer.nodesPerSlot) {
      throw RangeError.range(nodeCount, 1, _buffer.nodesPerSlot, 'nodeCount');
    }
  }

  /// Reserves [need] slots at once. Requests are served strictly in arrival
  /// order, so a request for two slots cannot be starved by single ones.
  Future<List<int>> _acquireSlots(
    int taskId,
    int need,
    AetherCancelToken? token,
  ) {
    if (token != null && token.isCancelled) {
      return Future<List<int>>.error(AetherTaskCancelled(taskId));
    }
    if (_slotWaiters.isEmpty && _buffer.freeSlotCount >= need) {
      return Future<List<int>>.value(_takeSlots(need));
    }
    final limit = config.maxPendingGrid;
    if (_slotWaiters.length >= limit) {
      final failure = AetherCapacityExceeded(
        taskId,
        'All ${_buffer.slotCount} slots are busy and $limit requests are '
        'already waiting',
      );
      _routeFailure(failure);
      return Future<List<int>>.error(failure);
    }
    final waiter = _SlotWaiter(need, token);
    if (token != null) {
      void listener(bool hard) {
        if (_slotWaiters.remove(waiter)) {
          waiter.completer.completeError(AetherTaskCancelled(taskId));
          // The head may have changed; the next waiter might now fit.
          _serveWaiters();
        }
      }

      waiter.listener = listener;
      token.addListener(listener);
    }
    _slotWaiters.addLast(waiter);
    return waiter.completer.future;
  }

  List<int> _takeSlots(int need) =>
      List<int>.generate(need, (_) => _buffer.acquireSlot(), growable: false);

  void _releaseSlot(int slot) {
    if (_buffer.isDisposed || !_buffer.isSlotInUse(slot)) {
      return;
    }
    _buffer.releaseSlot(slot);
    _serveWaiters();
  }

  void _serveWaiters() {
    while (_slotWaiters.isNotEmpty &&
        _buffer.freeSlotCount >= _slotWaiters.first.need) {
      final waiter = _slotWaiters.removeFirst();
      final listener = waiter.listener;
      if (listener != null) {
        waiter.cancelToken!.removeListener(listener);
      }
      waiter.completer.complete(_takeSlots(waiter.need));
    }
  }

  /// Current pool, memory and frame statistics.
  AetherStats stats() {
    _checkUsable();
    return AetherStats(
      pool: _pool.stats(),
      slotCount: _buffer.slotCount,
      freeSlots: _buffer.freeSlotCount,
      slotWaiters: _slotWaiters.length,
      activeContinuousGrids: _continuous.length,
      oneShotRunning: _oneShot.running,
      framesApplied: _stitcher.framesApplied,
      updatesApplied: _stitcher.updatesApplied,
      updatesCoalesced: _stitcher.updatesCoalesced,
      updatesDropped: _stitcher.updatesDropped,
    );
  }

  /// Shuts everything down, in order: stop continuous grids, drop pending
  /// frame updates, fail waiting, queued and one-shot work with
  /// [AetherPoolShutdown], kill every worker and wait for it to exit, then
  /// free the native memory. Safe to call more than once.
  Future<void> dispose() => _disposeFuture ??= _doDispose();

  Future<void> _doDispose() async {
    for (final grid in _continuous) {
      grid._running = false;
    }
    _stitcher.dispose();
    while (_slotWaiters.isNotEmpty) {
      final waiter = _slotWaiters.removeFirst();
      final listener = waiter.listener;
      if (listener != null) {
        waiter.cancelToken!.removeListener(listener);
      }
      waiter.completer.completeError(const AetherPoolShutdown(-1));
    }
    _oneShot.close();
    await _pool.shutdown();
    for (final grid in List<AetherContinuousGrid>.of(_continuous)) {
      grid._finishStop();
    }
    _continuous.clear();
    final observer = _observer;
    if (observer != null) {
      WidgetsBinding.instance.removeObserver(observer);
      _observer = null;
    }
    _buffer.dispose();
  }
}

/// The entry point: offload work without touching isolates or ports.
///
/// A static front for one default [AetherRuntime]. Call [initialize] once at
/// start-up:
///
/// ```dart
/// await AetherConcurrentRunner.initialize();
/// final n = await AetherConcurrentRunner.computeAsync(countPrimesTask(5000000));
/// ```
abstract final class AetherConcurrentRunner {
  static AetherConfig? _config;
  static AetherWorkerInit? _workerInit;
  static Future<AetherRuntime>? _initFuture;
  static AetherRuntime? _runtime;
  static Future<void>? _disposeFuture;

  /// Whether [initialize] has completed and [dispose] has not been called.
  static bool get isInitialized =>
      _runtime != null && !_runtime!.isDisposed && _disposeFuture == null;

  /// The active configuration, or null before [initialize].
  static AetherConfig? get config => _config;

  /// The default runtime. Throws [StateError] before [initialize].
  static AetherRuntime get runtime => _require();

  /// Allocates the memory grid and starts the warm worker pool.
  ///
  /// [workerInit] runs on every worker before it takes work, and again after
  /// a respawn; it must be a top-level or static function, or a closure that
  /// only captures sendable values. [onFailure] is called on the UI isolate,
  /// in a frame, for every failure except cancellations and shutdown.
  ///
  /// Calling this again with the same [config] and [workerInit] only updates
  /// [onFailure]. A different configuration throws [StateError] until
  /// [dispose] has completed. Throws [AetherWorkerInitFailure] if a worker
  /// cannot start; nothing is left allocated in that case.
  static Future<void> initialize({
    AetherConfig config = const AetherConfig(),
    AetherWorkerInit? workerInit,
    void Function(AetherFailure failure)? onFailure,
    @visibleForTesting AetherFrameScheduler? frameScheduler,
  }) async {
    if (AetherWorkerPool.isWorkerIsolate) {
      throw StateError('initialize() must be called on the main isolate');
    }
    final pendingDispose = _disposeFuture;
    if (pendingDispose != null) {
      await pendingDispose;
    }
    final existing = _initFuture;
    if (existing != null) {
      if (config == _config && workerInit == _workerInit) {
        final runtime = await existing;
        if (_disposeFuture != null) {
          throw StateError(
            'dispose() was called while initialize() was running',
          );
        }
        if (onFailure != null) {
          runtime.onFailure = onFailure;
        }
        return;
      }
      throw StateError(
        'AetherConcurrentRunner is already initialized with a different '
        'configuration. Call dispose() first.',
      );
    }
    _config = config;
    _workerInit = workerInit;
    final future = AetherRuntime._start(
      config: config,
      workerInit: workerInit,
      onFailure: onFailure,
      observeLifecycle: frameScheduler == null,
      frameScheduler: frameScheduler,
      onDetached: dispose,
    );
    _initFuture = future;
    try {
      _runtime = await future;
    } on Object {
      _initFuture = null;
      _config = null;
      _workerInit = null;
      rethrow;
    }
    if (_disposeFuture != null) {
      throw StateError('dispose() was called while initialize() was running');
    }
  }

  static AetherRuntime _require() {
    final runtime = _runtime;
    if (runtime == null || runtime.isDisposed || _disposeFuture != null) {
      throw StateError(
        'AetherConcurrentRunner is not initialized. '
        'Call AetherConcurrentRunner.initialize() first.',
      );
    }
    return runtime;
  }

  /// Waits for a running [initialize], then returns the default runtime.
  static Future<AetherRuntime> _ready() async {
    final initializing = _initFuture;
    if (_runtime == null && initializing != null && _disposeFuture == null) {
      await initializing;
    }
    return _require();
  }

  /// See [AetherRuntime.computeAsync]. On a worker, runs [task] inline so
  /// nested calls cannot deadlock the pool.
  static Future<T> computeAsync<T>(
    FutureOr<T> Function() task, {
    AetherPriority priority = AetherPriority.normal,
    Duration? timeout,
    AetherCancelToken? cancelToken,
  }) {
    if (AetherWorkerPool.isWorkerIsolate) {
      return Future<T>.sync(task);
    }
    if (_runtime == null && _initFuture != null) {
      return _ready().then(
        (runtime) => runtime.computeAsync<T>(
          task,
          priority: priority,
          timeout: timeout,
          cancelToken: cancelToken,
        ),
      );
    }
    return _require().computeAsync<T>(
      task,
      priority: priority,
      timeout: timeout,
      cancelToken: cancelToken,
    );
  }

  /// See [AetherRuntime.computeOnce].
  static Future<T> computeOnce<T>(
    FutureOr<T> Function() task, {
    Duration? timeout,
    AetherCancelToken? cancelToken,
  }) async {
    if (AetherWorkerPool.isWorkerIsolate) {
      return task();
    }
    final runtime = await _ready();
    return runtime.computeOnce<T>(
      task,
      timeout: timeout,
      cancelToken: cancelToken,
    );
  }

  /// See [AetherRuntime.computeMap].
  static Future<List<R>> computeMap<T, R>(
    List<T> items,
    R Function(T item) transform, {
    int? chunkSize,
    AetherPriority priority = AetherPriority.normal,
    Duration? timeout,
    AetherCancelToken? cancelToken,
  }) async {
    if (AetherWorkerPool.isWorkerIsolate) {
      return <R>[for (final item in items) transform(item)];
    }
    final runtime = await _ready();
    return runtime.computeMap<T, R>(
      items,
      transform,
      chunkSize: chunkSize,
      priority: priority,
      timeout: timeout,
      cancelToken: cancelToken,
    );
  }

  /// See [AetherRuntime.computeWithProgress].
  static Future<T> computeWithProgress<T>(
    AetherProgressTask<T> task, {
    required void Function(double fraction) onProgress,
    AetherPriority priority = AetherPriority.normal,
    Duration? timeout,
    AetherCancelToken? cancelToken,
  }) async {
    if (AetherWorkerPool.isWorkerIsolate) {
      return task((_) {});
    }
    final runtime = await _ready();
    return runtime.computeWithProgress<T>(
      task,
      onProgress: onProgress,
      priority: priority,
      timeout: timeout,
      cancelToken: cancelToken,
    );
  }

  /// See [AetherRuntime.computeGrid].
  static Future<AetherPayload> computeGrid({
    required AetherGridKernel kernel,
    required int nodeCount,
    AetherSlotCallback? prepare,
    AetherSlotCallback? onApplied,
    bool clearSlot = true,
    AetherPriority priority = AetherPriority.normal,
    Duration? timeout,
    AetherCancelToken? cancelToken,
  }) async {
    if (AetherWorkerPool.isWorkerIsolate) {
      throw StateError(
        'computeGrid() cannot run on a worker: memory slots are owned by the '
        'main isolate. Use computeAsync() for nested work.',
      );
    }
    final runtime = await _ready();
    return runtime.computeGrid(
      kernel: kernel,
      nodeCount: nodeCount,
      prepare: prepare,
      onApplied: onApplied,
      clearSlot: clearSlot,
      priority: priority,
      timeout: timeout,
      cancelToken: cancelToken,
    );
  }

  /// See [AetherRuntime.startContinuous].
  static Future<AetherContinuousGrid> startContinuous({
    required AetherStepKernel kernel,
    required int nodeCount,
    AetherSlotCallback? seed,
    AetherPriority priority = AetherPriority.high,
    Duration? stepTimeout,
    AetherCancelToken? cancelToken,
  }) async {
    if (AetherWorkerPool.isWorkerIsolate) {
      throw StateError(
        'startContinuous() cannot run on a worker: memory slots are owned by '
        'the main isolate.',
      );
    }
    final runtime = await _ready();
    return runtime.startContinuous(
      kernel: kernel,
      nodeCount: nodeCount,
      seed: seed,
      priority: priority,
      stepTimeout: stepTimeout,
      cancelToken: cancelToken,
    );
  }

  /// See [AetherRuntime.stats].
  static AetherStats stats() => _require().stats();

  /// Shuts the default runtime down (see [AetherRuntime.dispose]). Safe to
  /// call more than once; [initialize] may be called again after it
  /// completes. Runs automatically on [AppLifecycleState.detached].
  static Future<void> dispose() {
    final running = _disposeFuture;
    if (running != null) {
      return running;
    }
    if (_initFuture == null) {
      return Future<void>.value();
    }
    final future = _doDispose();
    _disposeFuture = future;
    return future;
  }

  static Future<void> _doDispose() async {
    AetherRuntime? runtime;
    try {
      runtime = await _initFuture;
    } on Object {
      // A failed initialize already cleaned up after itself.
    }
    await runtime?.dispose();
    _runtime = null;
    _config = null;
    _workerInit = null;
    _initFuture = null;
    _disposeFuture = null;
  }
}

/// A running double-buffered simulation started by
/// [AetherRuntime.startContinuous].
final class AetherContinuousGrid {
  AetherContinuousGrid._({
    required AetherRuntime runtime,
    required AetherStepKernel kernel,
    required this.nodeCount,
    required AetherPriority priority,
    required Duration? stepTimeout,
    required int front,
    required int back,
    required this.notifier,
  }) : _runtime = runtime,
       _kernel = kernel,
       _priority = priority,
       _stepTimeout = stepTimeout,
       _front = front,
       _back = back;

  /// Consecutive failed steps after which the grid stops itself.
  static const int maxConsecutiveFailures = 3;

  final AetherRuntime _runtime;
  final AetherStepKernel _kernel;
  final AetherPriority _priority;
  final Duration? _stepTimeout;

  /// Nodes simulated per step.
  final int nodeCount;

  /// Fires once per applied step; `value` is the front slot.
  final AetherSlotNotifier notifier;

  final Completer<void> _stopped = Completer<void>();
  int _front;
  int _back;
  bool _running = true;
  bool _paused = false;
  bool _inFlight = false;
  bool _waitingForFrames = false;
  int _steps = 0;
  int _consecutiveFailures = 0;
  AetherFailure? _lastFailure;

  /// Steps applied so far.
  int get steps => _steps;

  /// Whether new steps are being scheduled.
  bool get isRunning => _running && !_paused;

  /// Whether [pause] is in effect.
  bool get isPaused => _paused;

  /// The most recent step failure, if any.
  AetherFailure? get lastFailure => _lastFailure;

  /// Completes once the grid has stopped and released its slots.
  Future<void> get done => _stopped.future;

  /// Stops scheduling steps; the step in flight still lands.
  void pause() {
    _paused = true;
  }

  /// Continues after [pause].
  void resume() {
    if (!_paused) {
      return;
    }
    _paused = false;
    _submitStep();
  }

  /// Stops the grid. Completes once the step in flight (if any) is finished
  /// and both slots are released.
  Future<void> stop() {
    _running = false;
    if (!_inFlight) {
      _finishStop();
    }
    return _stopped.future;
  }

  void _submitStep() {
    if (!_running || _paused || _inFlight || _runtime.isDisposed) {
      return;
    }
    if (!_runtime._stitcher.framesEnabled) {
      // One step per frame: with no frames (app hidden or paused) the grid
      // waits, instead of spinning a core in the background.
      if (!_waitingForFrames) {
        _waitingForFrames = true;
        _runtime._stitcher.whenFramesResume(() {
          _waitingForFrames = false;
          _submitStep();
        });
      }
      return;
    }
    _inFlight = true;
    final buffer = _runtime._buffer;
    final payload = AetherPayload(
      taskId: _runtime._pool.reserveTaskId(),
      slotIndex: _back,
      nodeCount: nodeCount,
      nodeStride: buffer.nodeStride,
      priority: _priority,
    );
    buffer.writeHeader(payload);
    _runtime._pool
        .runStep(
          _back,
          _front,
          _kernel,
          priority: _priority,
          timeout: _stepTimeout,
          taskId: payload.taskId,
        )
        .then<void>((_) => _onStepDone(payload), onError: _onStepFailed);
  }

  void _onStepDone(AetherPayload payload) {
    if (_runtime.isDisposed) {
      _inFlight = false;
      _finishStop();
      return;
    }
    _runtime._stitcher.enqueue(
      AetherCallbackUpdate(
        key: payload.slotIndex,
        onApply: () {
          final previousFront = _front;
          _front = _back;
          _back = previousFront;
          _steps++;
          _consecutiveFailures = 0;
          notifier.publish(_runtime._buffer.slotView(_front), payload);
        },
        onAfterFrame: _afterStep,
        onSuperseded: _afterStep,
      ),
    );
  }

  void _afterStep() {
    _inFlight = false;
    if (_running) {
      _submitStep();
    } else {
      _finishStop();
    }
  }

  void _onStepFailed(Object error) {
    _inFlight = false;
    if (error is AetherFailure) {
      _lastFailure = error;
    }
    _consecutiveFailures++;
    if (!_running ||
        _runtime.isDisposed ||
        _consecutiveFailures >= maxConsecutiveFailures) {
      _running = false;
      _finishStop();
      return;
    }
    // Retry on the next frame rather than immediately, so a failing kernel
    // cannot spin the pool.
    _inFlight = true;
    _runtime._stitcher.enqueue(
      AetherCallbackUpdate(onAfterFrame: _afterStep, onSuperseded: _afterStep),
    );
  }

  void _finishStop() {
    if (_stopped.isCompleted) {
      return;
    }
    _running = false;
    notifier.deactivate();
    _runtime
      .._releaseSlot(_front)
      .._releaseSlot(_back);
    _runtime._continuous.remove(this);
    _stopped.complete();
  }
}
