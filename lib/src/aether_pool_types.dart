// Platform-neutral pool types: kernel signatures, settings and statistics.
// Shared by the isolate (native) and main-thread (web) pool backends.

import 'dart:async';
import 'dart:typed_data';

import 'aether_default_workers_io.dart'
    if (dart.library.js_interop) 'aether_default_workers_web.dart';
import 'aether_payload.dart';

/// A kernel that transforms one slot in place on a worker.
typedef AetherGridKernel =
    FutureOr<void> Function(Float32List slot, AetherPayload payload);

/// A kernel that reads [previous] and writes the next state into [next].
typedef AetherStepKernel =
    FutureOr<void> Function(
      Float32List previous,
      Float32List next,
      AetherPayload payload,
    );

/// A task that reports its progress, a fraction from 0.0 to 1.0, through
/// [report]. Reports are throttled to about 60 per second; 1.0 always
/// gets through.
typedef AetherProgressTask<T> =
    FutureOr<T> Function(void Function(double fraction) report);

/// Minimum time between two progress reports sent by a task.
const int aetherProgressIntervalMicros = 16000;

/// Runs once on every worker when it starts (and again after a respawn).
typedef AetherWorkerInit = FutureOr<void> Function();

/// Pool settings. Every field has a working default.
final class AetherPoolConfig {
  /// Creates a pool configuration.
  const AetherPoolConfig({
    this.workerCount,
    this.taskTimeout = const Duration(seconds: 30),
    this.cleanupInterval = const Duration(seconds: 1),
    this.agingInterval = const Duration(milliseconds: 250),
    this.watchdogTick = const Duration(milliseconds: 10),
    this.latencySampleCapacity = 1024,
    this.enableTimeline = true,
  });

  /// Number of workers. `null` means `max(1, processors − 2)`: Flutter
  /// renders on two threads (UI and raster), and leaving a core for each
  /// keeps frames smooth while every worker is busy. Set it higher for
  /// throughput-bound work that does not run during animations. Ignored on
  /// the web, where tasks run on the UI thread.
  final int? workerCount;

  /// Default per-task timeout. `null` disables timeouts.
  final Duration? taskTimeout;

  /// How often queues are compacted and dead workers re-checked.
  final Duration cleanupInterval;

  /// How long a task waits before moving up one priority level.
  final Duration agingInterval;

  /// How often running tasks are checked against their timeout.
  final Duration watchdogTick;

  /// How many recent latency samples [AetherPoolStats] percentiles use.
  final int latencySampleCapacity;

  /// Emit a `dart:developer` timeline event per task (visible in DevTools).
  final bool enableTimeline;

  /// The worker count actually used.
  int get resolvedWorkerCount => workerCount ?? aetherDefaultWorkerCount();

  @override
  bool operator ==(Object other) =>
      other is AetherPoolConfig &&
      other.workerCount == workerCount &&
      other.taskTimeout == taskTimeout &&
      other.cleanupInterval == cleanupInterval &&
      other.agingInterval == agingInterval &&
      other.watchdogTick == watchdogTick &&
      other.latencySampleCapacity == latencySampleCapacity &&
      other.enableTimeline == enableTimeline;

  @override
  int get hashCode => Object.hash(
    workerCount,
    taskTimeout,
    cleanupInterval,
    agingInterval,
    watchdogTick,
    latencySampleCapacity,
    enableTimeline,
  );
}

/// A snapshot of pool health and latency.
final class AetherPoolStats {
  /// Creates a stats snapshot.
  const AetherPoolStats({
    required this.workerCount,
    required this.idleWorkers,
    required this.busyWorkers,
    required this.startingWorkers,
    required this.queuedHigh,
    required this.queuedNormal,
    required this.queuedLow,
    required this.completedTasks,
    required this.failedTasks,
    required this.respawns,
    required this.latencySamples,
    required this.p50Micros,
    required this.p99Micros,
    required this.maxMicros,
  });

  /// Configured number of workers.
  final int workerCount;

  /// Workers waiting for work.
  final int idleWorkers;

  /// Workers running a task.
  final int busyWorkers;

  /// Workers being spawned or respawned.
  final int startingWorkers;

  /// Tasks waiting in the high queue.
  final int queuedHigh;

  /// Tasks waiting in the normal queue.
  final int queuedNormal;

  /// Tasks waiting in the low queue.
  final int queuedLow;

  /// Tasks that finished successfully since start.
  final int completedTasks;

  /// Tasks that failed (any [AetherFailure]) since start.
  final int failedTasks;

  /// Workers respawned after a crash, timeout or hard cancel.
  final int respawns;

  /// Number of samples behind the percentiles.
  final int latencySamples;

  /// Median submit-to-result time in microseconds.
  final int p50Micros;

  /// 99th percentile submit-to-result time in microseconds.
  final int p99Micros;

  /// Slowest recorded submit-to-result time in microseconds.
  final int maxMicros;

  /// Total queued tasks.
  int get queued => queuedHigh + queuedNormal + queuedLow;

  @override
  String toString() =>
      'AetherPoolStats(workers $workerCount: $busyWorkers busy, '
      '$idleWorkers idle, $startingWorkers starting; queued $queued; '
      'done $completedTasks, failed $failedTasks, respawns $respawns; '
      'p50 ${p50Micros}us p99 ${p99Micros}us max ${maxMicros}us)';
}
