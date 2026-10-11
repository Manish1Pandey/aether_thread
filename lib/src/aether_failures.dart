// Typed failures and cancellation. Every way a task can fail maps to exactly
// one AetherFailure subclass, so callers can switch over them exhaustively.

/// Base class of every failure Aether Thread reports.
///
/// A failure completes the task's `Future` with an error and is also passed
/// to the global `onFailure` callback (except [AetherTaskCancelled], which
/// the caller asked for).
sealed class AetherFailure implements Exception {
  const AetherFailure(this.taskId, this.message);

  /// Id of the task that failed, or -1 when the failure is not tied to one.
  final int taskId;

  /// Human-readable description.
  final String message;

  @override
  String toString() => '$runtimeType(task $taskId): $message';
}

/// The task's own code threw.
final class AetherTaskException extends AetherFailure {
  /// Creates a failure for an exception thrown inside the task.
  AetherTaskException(int taskId, this.error, String remoteStackTrace)
    : remoteStackTrace = StackTrace.fromString(remoteStackTrace),
      super(taskId, 'Task threw: $error');

  /// The thrown object, or its `toString()` if it could not be sent back.
  final Object error;

  /// Stack trace captured on the worker isolate.
  final StackTrace remoteStackTrace;
}

/// The worker isolate died while running the task.
final class AetherWorkerCrash extends AetherFailure {
  /// Creates a failure for a worker that exited mid-task.
  const AetherWorkerCrash(int taskId, this.workerIndex, String detail)
    : super(taskId, detail);

  /// Index of the worker that died. It is respawned automatically.
  final int workerIndex;
}

/// The task ran past its timeout, so its worker was killed and respawned.
final class AetherTaskTimeout extends AetherFailure {
  /// Creates a timeout failure.
  AetherTaskTimeout(int taskId, this.timeout)
    : super(taskId, 'Task exceeded ${timeout.inMilliseconds} ms');

  /// The timeout that was exceeded.
  final Duration timeout;
}

/// The task was cancelled through its [AetherCancelToken].
final class AetherTaskCancelled extends AetherFailure {
  /// Creates a cancellation failure.
  const AetherTaskCancelled(int taskId) : super(taskId, 'Task cancelled');
}

/// The task closure captures something that cannot cross isolates
/// (a socket, a `ReceivePort`, a `Pointer`-backed finalizable, ...).
final class AetherUnsendableTask extends AetherFailure {
  /// Creates a failure for a closure that could not be sent.
  const AetherUnsendableTask(super.taskId, super.message);
}

/// The task finished, but its result cannot cross isolates.
final class AetherUnsendableResult extends AetherFailure {
  /// Creates a failure for a result that could not be sent back.
  const AetherUnsendableResult(super.taskId, super.message);
}

/// Every memory slot is busy and the wait queue is full.
final class AetherCapacityExceeded extends AetherFailure {
  /// Creates a capacity failure.
  const AetherCapacityExceeded(super.taskId, super.message);
}

/// The pool shut down before the task could finish.
final class AetherPoolShutdown extends AetherFailure {
  /// Creates a shutdown failure.
  const AetherPoolShutdown(int taskId) : super(taskId, 'Pool shut down');
}

/// A worker's `workerInit` failed or its isolate could not be spawned.
final class AetherWorkerInitFailure extends AetherFailure {
  /// Creates an initialisation failure.
  const AetherWorkerInitFailure(this.workerIndex, String detail)
    : super(-1, detail);

  /// Index of the worker that failed to start.
  final int workerIndex;
}

/// Turns the VM's "object is unsendable" error into an actionable message
/// naming the offending class.
///
/// [what] says what failed to cross ("task closure", "result").
String describeUnsendable(ArgumentError error, String what) {
  final raw = '${error.message}';
  final cls = RegExp(r'Class: ([\w$]+)').firstMatch(raw)?.group(1);
  final lib = RegExp(r"Library:'([^']+)'").firstMatch(raw)?.group(1);
  if (cls == null) {
    return 'The $what cannot be sent to another isolate: $raw';
  }
  final name = cls.split('@').first;
  final from = lib == null ? '' : ' (from $lib)';
  final hint = what == 'result'
      ? 'Return plain data (numbers, strings, lists, maps, typed data or '
            'your own classes made of those) instead.'
      : 'Closures capture every variable of the scope they are written in. '
            'Build the task in a top-level or static function that takes '
            'only plain data as parameters, and pass those values in.';
  return 'The $what holds a `$name`$from, which cannot be sent to another '
      'isolate. $hint';
}

/// Lets a caller cancel one or more tasks.
///
/// Pass the same token to several calls to cancel them together. A token
/// cannot be reset once cancelled.
final class AetherCancelToken {
  /// Creates a token that is not cancelled.
  AetherCancelToken();

  final List<void Function(bool hard)> _listeners = <void Function(bool)>[];
  bool _cancelled = false;
  bool _hard = false;

  /// Whether [cancel] has been called.
  bool get isCancelled => _cancelled;

  /// Whether the cancellation asked for running work to be killed.
  bool get isHard => _hard;

  /// Cancels every task holding this token.
  ///
  /// Queued tasks fail right away with [AetherTaskCancelled]. A running
  /// task's result is discarded when it finishes; with [hard] its worker is
  /// killed and respawned instead.
  void cancel({bool hard = false}) {
    if (_cancelled) {
      return;
    }
    _cancelled = true;
    _hard = hard;
    final listeners = List<void Function(bool)>.of(_listeners);
    _listeners.clear();
    for (final listener in listeners) {
      listener(hard);
    }
  }

  /// Registers [listener]; runs it immediately if already cancelled.
  void addListener(void Function(bool hard) listener) {
    if (_cancelled) {
      listener(_hard);
    } else {
      _listeners.add(listener);
    }
  }

  /// Removes a listener added with [addListener].
  void removeListener(void Function(bool hard) listener) {
    _listeners.remove(listener);
  }
}
