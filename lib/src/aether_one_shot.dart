// One-shot tasks on a fresh isolate.
//
// A warm worker has to copy its reply into the owning isolate. A fresh
// isolate can instead end with `Isolate.exit`, which hands the result's
// memory over without copying. For a one-off job that returns a very large
// result that is faster than any warm pool, so this path exists for it.
// Unlike a bare `Isolate.run`, it keeps Aether's timeouts (pausable while
// the app is in the background), cancellation and typed failures.
//
// Depends only on dart: libraries.

import 'dart:async';
import 'dart:isolate';

import 'aether_failures.dart';
import 'aether_pool_manager.dart' show markAetherWorkerIsolate;

final class _OneShotBoot {
  const _OneShotBoot(this.task, this.replyPort);

  final FutureOr<Object?> Function() task;
  final SendPort replyPort;
}

// Everything arrives on one port, so its order is kept:
//   [0, value]            the result
//   [1, error, stack]     the task threw
//   [2, detail]           the result could not be sent
//   [String, String?]     an uncaught error (from `onError`)
//   null                  the isolate exited (from `onExit`)
Future<void> _oneShotMain(_OneShotBoot boot) async {
  markAetherWorkerIsolate();
  Object? value;
  try {
    value = await boot.task();
  } catch (error, stack) {
    try {
      Isolate.exit(boot.replyPort, <Object?>[1, error, stack.toString()]);
    } on ArgumentError {
      Isolate.exit(boot.replyPort, <Object?>[
        1,
        error.toString(),
        stack.toString(),
      ]);
    }
  }
  try {
    Isolate.exit(boot.replyPort, <Object?>[0, value]);
  } on ArgumentError catch (sendError) {
    Isolate.exit(boot.replyPort, <Object?>[
      2,
      describeUnsendable(sendError, 'result'),
    ]);
  }
}

/// A timeout whose clock can be frozen while the app is in the background.
final class _PausableTimeout {
  _PausableTimeout(this.limit, this.onExpired) {
    _clock.start();
    _arm();
  }

  final Duration limit;
  final void Function() onExpired;
  final Stopwatch _clock = Stopwatch();
  Timer? _timer;

  void _arm() {
    final remaining = limit - _clock.elapsed;
    _timer = Timer(remaining.isNegative ? Duration.zero : remaining, onExpired);
  }

  void pause() {
    _timer?.cancel();
    _timer = null;
    _clock.stop();
  }

  void resume() {
    if (_timer != null) {
      return;
    }
    _clock.start();
    _arm();
  }

  void cancel() {
    _timer?.cancel();
    _timer = null;
    _clock.stop();
  }
}

/// Runs one-shot tasks, each on its own short-lived isolate.
final class AetherOneShotRunner {
  // Each running task's kill function, so close() can record the reason.
  final Set<void Function(AetherFailure reason)> _live =
      <void Function(AetherFailure reason)>{};
  final Set<_PausableTimeout> _timeouts = <_PausableTimeout>{};
  bool _closed = false;
  bool _paused = false;

  /// Number of one-shot isolates currently running.
  int get running => _live.length;

  /// Freezes every running task's timeout, for example while the OS has
  /// suspended the app.
  void pauseTimeouts() {
    _paused = true;
    for (final timeout in _timeouts) {
      timeout.pause();
    }
  }

  /// Restarts the clocks frozen by [pauseTimeouts].
  void resumeTimeouts() {
    _paused = false;
    for (final timeout in _timeouts) {
      timeout.resume();
    }
  }

  /// Runs [task] on a new isolate and completes with its result, which is
  /// transferred without copying.
  ///
  /// Fails with [AetherTaskTimeout] (the isolate is killed), with
  /// [AetherTaskCancelled] when [cancelToken] is cancelled (also killed),
  /// [AetherTaskException], [AetherWorkerCrash], [AetherUnsendableTask],
  /// [AetherUnsendableResult] or [AetherPoolShutdown].
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
    final port = RawReceivePort();
    Isolate? isolate;
    AetherFailure? killReason;
    String? lastError;

    void fail(AetherFailure failure) {
      if (completer.isCompleted) {
        return;
      }
      completer.completeError(failure);
      if (failure is! AetherTaskCancelled && failure is! AetherPoolShutdown) {
        onFailure?.call(failure);
      }
    }

    void kill(AetherFailure reason) {
      killReason ??= reason;
      isolate?.kill(priority: Isolate.immediate);
    }

    void cancelListener(bool hard) => kill(AetherTaskCancelled(taskId));

    port.handler = (Object? message) {
      if (message == null) {
        fail(
          killReason ??
              AetherWorkerCrash(
                taskId,
                -1,
                lastError ?? 'One-shot isolate exited without a result',
              ),
        );
        return;
      }
      final list = message as List<Object?>;
      final tag = list[0];
      if (tag is! int) {
        // An uncaught error report: [error, stack].
        lastError = list.length > 1 && list[1] != null
            ? '${list[0]}\n${list[1]}'
            : '${list[0]}';
        return;
      }
      switch (tag) {
        case 0:
          if (!completer.isCompleted) {
            completer.complete(list[1]);
          }
        case 1:
          fail(AetherTaskException(taskId, list[1]!, list[2]! as String));
        default:
          fail(AetherUnsendableResult(taskId, list[1]! as String));
      }
    };

    try {
      isolate = await Isolate.spawn<_OneShotBoot>(
        _oneShotMain,
        _OneShotBoot(task, port.sendPort),
        onExit: port.sendPort,
        onError: port.sendPort,
        debugName: 'aether-one-shot-$taskId',
      );
    } on Object catch (error) {
      port.close();
      final AetherFailure failure = error is ArgumentError
          ? AetherUnsendableTask(
              taskId,
              describeUnsendable(error, 'task closure'),
            )
          : AetherWorkerCrash(taskId, -1, 'Isolate.spawn failed: $error');
      onFailure?.call(failure);
      throw failure;
    }
    final AetherFailure shutdown = AetherPoolShutdown(taskId);
    void killForShutdown(AetherFailure _) => kill(shutdown);
    _live.add(killForShutdown);
    if (_closed) {
      kill(shutdown);
    }
    _PausableTimeout? deadline;
    if (timeout != null) {
      deadline = _PausableTimeout(
        timeout,
        () => kill(AetherTaskTimeout(taskId, timeout)),
      );
      _timeouts.add(deadline);
      if (_paused) {
        deadline.pause();
      }
    }
    cancelToken?.addListener(cancelListener);
    try {
      return await completer.future as T;
    } finally {
      deadline?.cancel();
      _timeouts.remove(deadline);
      cancelToken?.removeListener(cancelListener);
      _live.remove(killForShutdown);
      port.close();
    }
  }

  /// Kills every running one-shot isolate; their tasks fail with
  /// [AetherPoolShutdown]. Later calls fail immediately.
  void close() {
    _closed = true;
    for (final kill in List<void Function(AetherFailure)>.of(_live)) {
      kill(const AetherPoolShutdown(-1));
    }
  }
}
