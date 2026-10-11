// MODULE 3 — The state stitcher.
//
// Worker results arrive on the UI isolate at arbitrary times, as port
// messages. Applying them there and then could change state halfway through
// a frame. The stitcher instead collects them and applies the whole batch at
// one fixed point: the transient-callback phase at the start of the next
// frame, before build. Widgets that the updates mark dirty then rebuild in
// that same frame. Slots are recycled in a post-frame callback, after the
// frame has finished reading them.

import 'dart:async';

import 'package:flutter/foundation.dart';
import 'package:flutter/scheduler.dart';

import 'aether_payload.dart';

/// One change to apply in a frame.
abstract class AetherFrameUpdate {
  /// Coalescing key (a slot index), or -1 for updates that never coalesce.
  /// Only the newest pending update per key is applied.
  int get key;

  /// Runs in the frame's transient-callback phase, before build.
  void apply();

  /// Runs in the same frame's post-frame phase, after paint.
  void afterFrame();

  /// Runs instead of [apply] and [afterFrame] when a newer update with the
  /// same key replaced this one, or when it was dropped or the stitcher was
  /// disposed. Release anything the update holds here.
  void superseded();
}

/// An [AetherFrameUpdate] made from callbacks.
final class AetherCallbackUpdate extends AetherFrameUpdate {
  /// Creates an update from callbacks. Omitted callbacks do nothing.
  AetherCallbackUpdate({
    this.key = -1,
    void Function()? onApply,
    void Function()? onAfterFrame,
    void Function()? onSuperseded,
  }) : _onApply = onApply,
       _onAfterFrame = onAfterFrame,
       _onSuperseded = onSuperseded;

  @override
  final int key;
  final void Function()? _onApply;
  final void Function()? _onAfterFrame;
  final void Function()? _onSuperseded;

  @override
  void apply() => _onApply?.call();

  @override
  void afterFrame() => _onAfterFrame?.call();

  @override
  void superseded() => _onSuperseded?.call();
}

/// Where frame callbacks come from. The default uses [SchedulerBinding];
/// tests can drive frames by hand.
abstract interface class AetherFrameScheduler {
  /// Registers [callback] for the next frame's transient-callback phase and
  /// makes sure that frame is scheduled.
  void scheduleFrameCallback(void Function(Duration timeStamp) callback);

  /// Registers [callback] for the end of the current frame.
  void addPostFrameCallback(void Function(Duration timeStamp) callback);

  /// Whether frames are being produced. False while the app is hidden or
  /// paused; updates are then applied without waiting for a frame.
  bool get framesEnabled;
}

final class _BindingFrameScheduler implements AetherFrameScheduler {
  const _BindingFrameScheduler();

  @override
  void scheduleFrameCallback(void Function(Duration) callback) {
    SchedulerBinding.instance.scheduleFrameCallback(callback);
  }

  @override
  void addPostFrameCallback(void Function(Duration) callback) {
    SchedulerBinding.instance.addPostFrameCallback(callback);
  }

  @override
  bool get framesEnabled => SchedulerBinding.instance.framesEnabled;
}

/// Batches worker results and applies them between frames.
final class AetherStateStitcher {
  /// Creates a stitcher for keys `0 .. keySpace − 1` holding at most
  /// [maxPending] updates between frames.
  ///
  /// [maxPending] must exceed [keySpace], so keyed updates (at most one per
  /// key) always fit and only unkeyed notices can ever be dropped.
  AetherStateStitcher({
    required int keySpace,
    required int maxPending,
    AetherFrameScheduler? scheduler,
  }) : _scheduler = scheduler ?? const _BindingFrameScheduler(),
       _keyIndex = Int32List(keySpace)..fillRange(0, keySpace, -1),
       _active = List<AetherFrameUpdate?>.filled(maxPending, null),
       _spare = List<AetherFrameUpdate?>.filled(maxPending, null) {
    if (maxPending <= keySpace) {
      throw ArgumentError.value(
        maxPending,
        'maxPending',
        'must be greater than keySpace ($keySpace)',
      );
    }
  }

  final AetherFrameScheduler _scheduler;
  final Int32List _keyIndex;
  List<AetherFrameUpdate?> _active;
  List<AetherFrameUpdate?> _spare;
  int _count = 0;
  int _applyingCount = 0;
  bool _frameScheduled = false;
  bool _drainScheduled = false;
  int _frameToken = 0;
  bool _disposed = false;
  int _frames = 0;
  int _applied = 0;
  int _coalesced = 0;
  int _dropped = 0;

  /// Updates waiting for the next frame.
  int get pendingCount => _count;

  /// Frames in which at least one update was applied.
  int get framesApplied => _frames;

  /// Updates applied so far.
  int get updatesApplied => _applied;

  /// Updates replaced by a newer one with the same key before a frame.
  int get updatesCoalesced => _coalesced;

  /// Unkeyed updates dropped because the pending list was full.
  int get updatesDropped => _dropped;

  /// Whether a frame callback is registered and not yet run.
  bool get isFrameScheduled => _frameScheduled;

  /// Queues [update] for the next frame.
  ///
  /// A pending update with the same key is replaced (and told it was
  /// superseded). When the list is full, the oldest unkeyed update is dropped.
  void enqueue(AetherFrameUpdate update) {
    if (_disposed) {
      update.superseded();
      return;
    }
    final key = update.key;
    if (key >= 0) {
      final existing = _keyIndex[key];
      if (existing >= 0) {
        final old = _active[existing]!;
        _active[existing] = update;
        _coalesced++;
        old.superseded();
        _schedule();
        return;
      }
    }
    if (_count == _active.length) {
      _dropOldestUnkeyed();
    }
    _active[_count] = update;
    if (key >= 0) {
      _keyIndex[key] = _count;
    }
    _count++;
    _schedule();
  }

  void _dropOldestUnkeyed() {
    var victim = -1;
    for (var i = 0; i < _count; i++) {
      if (_active[i]!.key < 0) {
        victim = i;
        break;
      }
    }
    // maxPending > keySpace guarantees an unkeyed entry exists when full.
    final dropped = _active[victim]!;
    for (var i = victim; i < _count - 1; i++) {
      final moved = _active[i + 1]!;
      _active[i] = moved;
      if (moved.key >= 0) {
        _keyIndex[moved.key] = i;
      }
    }
    _count--;
    _active[_count] = null;
    _dropped++;
    dropped.superseded();
  }

  void _schedule() {
    if (!_scheduler.framesEnabled) {
      // No frame will come while the app is hidden or paused. Apply soon,
      // outside a frame, so results and slots are not held hostage.
      if (!_drainScheduled) {
        _drainScheduled = true;
        Timer.run(_drainWithoutFrame);
      }
      return;
    }
    if (_frameScheduled) {
      return;
    }
    _frameScheduled = true;
    final token = _frameToken;
    _scheduler.scheduleFrameCallback((stamp) => _onFrame(stamp, token));
  }

  /// Whether the scheduler is currently producing frames.
  bool get framesEnabled => _scheduler.framesEnabled;

  /// Runs [callback] at the start of the next frame, i.e. once frames are
  /// produced again. Used to pause frame-paced work in the background
  /// without polling.
  void whenFramesResume(void Function() callback) {
    _scheduler.scheduleFrameCallback((_) => callback());
  }

  /// Applies and recycles everything pending right now, without a frame.
  ///
  /// Called when frames stop (the app is hidden or paused). A frame callback
  /// already registered becomes stale and does nothing when it finally runs.
  void flushWithoutFrame() {
    if (_disposed || _count == 0) {
      return;
    }
    _frameToken++;
    _frameScheduled = false;
    _drainWithoutFrame();
  }

  void _drainWithoutFrame() {
    _drainScheduled = false;
    if (_disposed) {
      return;
    }
    final count = _count;
    // Detach the batch first: callbacks may enqueue more.
    final batch = List<AetherFrameUpdate>.generate(count, (i) => _active[i]!);
    for (var i = 0; i < count; i++) {
      _active[i] = null;
    }
    _count = 0;
    _keyIndex.fillRange(0, _keyIndex.length, -1);
    for (final update in batch) {
      _guard(update.apply, 'applying a worker result');
    }
    for (final update in batch) {
      _guard(update.afterFrame, 'recycling after an update');
    }
    if (count > 0) {
      _applied += count;
    }
  }

  void _onFrame(Duration timeStamp, int token) {
    if (token != _frameToken) {
      return;
    }
    _frameScheduled = false;
    if (_disposed) {
      return;
    }
    final draining = _active;
    final count = _count;
    _active = _spare;
    _spare = draining;
    _count = 0;
    _keyIndex.fillRange(0, _keyIndex.length, -1);
    _applyingCount = count;
    for (var i = 0; i < count; i++) {
      _guard(draining[i]!.apply, 'applying a worker result');
    }
    if (count > 0) {
      _frames++;
      _applied += count;
    }
    _scheduler.addPostFrameCallback(_afterFrame);
  }

  void _afterFrame(Duration timeStamp) {
    final applied = _spare;
    for (var i = 0; i < _applyingCount; i++) {
      final update = applied[i]!;
      applied[i] = null;
      _guard(update.afterFrame, 'recycling after a frame');
    }
    _applyingCount = 0;
  }

  void _guard(void Function() action, String what) {
    try {
      action();
    } catch (error, stack) {
      FlutterError.reportError(
        FlutterErrorDetails(
          exception: error,
          stack: stack,
          library: 'aether_thread',
          context: ErrorDescription('while $what'),
        ),
      );
    }
  }

  /// Supersedes every pending update and stops scheduling frames.
  void dispose() {
    if (_disposed) {
      return;
    }
    _disposed = true;
    for (var i = 0; i < _count; i++) {
      final update = _active[i]!;
      _active[i] = null;
      _guard(update.superseded, 'disposing a pending update');
    }
    _count = 0;
  }
}

/// Exposes the current contents of a slot to widgets.
///
/// `value` is a view over shared native memory; read it during build or
/// paint, never keep it past the next change notification.
final class AetherSlotNotifier extends ChangeNotifier
    implements ValueListenable<Float32List> {
  /// Creates a notifier showing [initial].
  AetherSlotNotifier(Float32List initial, AetherPayload payload)
    : _value = initial,
      _payload = payload;

  Float32List _value;
  AetherPayload _payload;
  int _version = 0;
  bool _active = true;

  @override
  Float32List get value => _value;

  /// Header of the data currently shown.
  AetherPayload get payload => _payload;

  /// Increases by one on every published update.
  int get version => _version;

  /// False once the owner has stopped and the memory may be reused.
  bool get isActive => _active;

  /// Shows [view] and notifies listeners. Called by the runner inside a
  /// frame's transient-callback phase.
  void publish(Float32List view, AetherPayload payload) {
    _value = view;
    _payload = payload;
    _version++;
    notifyListeners();
  }

  /// Marks the notifier inactive and notifies listeners one last time.
  void deactivate() {
    if (!_active) {
      return;
    }
    _active = false;
    _version++;
    notifyListeners();
  }
}
