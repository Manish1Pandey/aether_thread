// Web backend of one-shot tasks. There are no isolates on the web, so a
// one-shot task runs on the UI thread in its own event-loop turn, with the
// same API as the native runner: timeouts (pausable), cancellation and typed
// failures. A timeout or cancel abandons the task; code already running
// cannot be stopped.

import 'dart:async';

import 'aether_deadline.dart';
import 'aether_failures.dart';
import 'aether_pool_web.dart' show AetherTaskMarker, aetherWorkerZoneKey;

/// Runs one-shot tasks. On the web each runs on the UI thread.
final class AetherOneShotRunner {
  final Set<void Function(AetherFailure reason)> _live =
      <void Function(AetherFailure reason)>{};
  final Set<AetherDeadline> _deadlines = <AetherDeadline>{};
  bool _closed = false;
  bool _paused = false;

  /// Number of one-shot tasks currently running.
  int get running => _live.length;

  /// Freezes every running task's timeout.
  void pauseTimeouts() {
    _paused = true;
    for (final deadline in _deadlines) {
      deadline.pause();
    }
  }

  /// Restarts the clocks frozen by [pauseTimeouts].
  void resumeTimeouts() {
    _paused = false;
    for (final deadline in _deadlines) {
      deadline.resume();
    }
  }

  /// Runs [task] in its own event-loop turn and completes with its result.
  ///
  /// Fails with [AetherTaskTimeout], [AetherTaskCancelled],
  /// [AetherTaskException] or [AetherPoolShutdown].
  Future<T> run<T>(
    FutureOr<T> Function() task, {
    required int taskId,
    Duration? timeout,
    AetherCancelToken? cancelToken,
    void Function(AetherFailure failure)? onFailure,
  }) async {
    if (_closed) {
      throw AetherPoolShutdown(taskId);
    }
    if (cancelToken != null && cancelToken.isCancelled) {
      throw AetherTaskCancelled(taskId);
    }
    final completer = Completer<Object?>();
    final marker = AetherTaskMarker();

    void fail(AetherFailure failure) {
      if (completer.isCompleted) {
        return;
      }
      completer.completeError(failure);
      if (failure is! AetherTaskCancelled && failure is! AetherPoolShutdown) {
        onFailure?.call(failure);
      }
    }

    void abandon(AetherFailure reason) => fail(reason);
    void cancelListener(bool hard) => abandon(AetherTaskCancelled(taskId));

    _live.add(abandon);
    AetherDeadline? deadline;
    if (timeout != null) {
      deadline = AetherDeadline(
        timeout,
        () => abandon(AetherTaskTimeout(taskId, timeout)),
        paused: _paused,
      );
      _deadlines.add(deadline);
    }
    cancelToken?.addListener(cancelListener);
    Timer.run(() {
      if (completer.isCompleted) {
        return;
      }
      final Future<Object?> result = runZoned(
        () => Future<Object?>.sync(task),
        zoneValues: <Object?, Object?>{aetherWorkerZoneKey: marker},
      );
      result.then<void>(
        (value) {
          if (!completer.isCompleted) {
            completer.complete(value);
          }
        },
        onError: (Object error, StackTrace stack) =>
            fail(AetherTaskException(taskId, error, stack.toString())),
      );
    });
    try {
      return await completer.future as T;
    } finally {
      marker.active = false;
      deadline?.cancel();
      _deadlines.remove(deadline);
      cancelToken?.removeListener(cancelListener);
      _live.remove(abandon);
    }
  }

  /// Abandons every running one-shot task with [AetherPoolShutdown]. Later
  /// calls fail immediately.
  void close() {
    _closed = true;
    for (final abandon in List<void Function(AetherFailure)>.of(_live)) {
      abandon(const AetherPoolShutdown(-1));
    }
  }
}
