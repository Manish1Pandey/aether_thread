// A timeout whose clock can be frozen while the app is in the background.

import 'dart:async';

/// Calls [onExpired] once [limit] of *running* time has passed. [pause] and
/// [resume] freeze and restart the clock.
final class AetherDeadline {
  /// Starts a deadline of [limit], paused from the start if [paused].
  AetherDeadline(this.limit, this.onExpired, {bool paused = false}) {
    if (!paused) {
      resume();
    }
  }

  /// Running time allowed before [onExpired] fires.
  final Duration limit;

  /// Called once when the deadline passes.
  final void Function() onExpired;

  final Stopwatch _clock = Stopwatch();
  Timer? _timer;
  bool _cancelled = false;

  /// Freezes the clock.
  void pause() {
    _timer?.cancel();
    _timer = null;
    _clock.stop();
  }

  /// Restarts the clock after [pause].
  void resume() {
    if (_timer != null || _cancelled) {
      return;
    }
    _clock.start();
    final remaining = limit - _clock.elapsed;
    _timer = Timer(remaining.isNegative ? Duration.zero : remaining, () {
      _timer = null;
      if (!_cancelled) {
        _cancelled = true;
        onExpired();
      }
    });
  }

  /// Stops the deadline for good.
  void cancel() {
    _cancelled = true;
    _timer?.cancel();
    _timer = null;
    _clock.stop();
  }
}
