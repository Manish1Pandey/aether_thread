@TestOn('vm')
library;

import 'package:aether_thread/aether_thread.dart';
import 'package:flutter/foundation.dart';
import 'package:flutter_test/flutter_test.dart';

import 'support.dart';

void main() {
  late ManualFrameScheduler frames;
  late AetherStateStitcher stitcher;
  late List<String> log;

  AetherCallbackUpdate update(String name, {int key = -1}) =>
      AetherCallbackUpdate(
        key: key,
        onApply: () => log.add('apply $name'),
        onAfterFrame: () => log.add('after $name'),
        onSuperseded: () => log.add('superseded $name'),
      );

  setUp(() {
    frames = ManualFrameScheduler();
    stitcher = AetherStateStitcher(
      keySpace: 4,
      maxPending: 6,
      scheduler: frames,
    );
    log = <String>[];
  });

  test('applies nothing until the frame, then the whole batch in order', () {
    stitcher
      ..enqueue(update('a'))
      ..enqueue(update('b', key: 1));
    expect(log, isEmpty);
    expect(stitcher.pendingCount, 2);
    frames.pumpFrame();
    expect(log, <String>['apply a', 'apply b', 'after a', 'after b']);
    expect(stitcher.framesApplied, 1);
    expect(stitcher.updatesApplied, 2);
    expect(stitcher.pendingCount, 0);
  });

  test('schedules exactly one frame callback per batch', () {
    for (var i = 0; i < 3; i++) {
      stitcher.enqueue(update('$i'));
    }
    expect(stitcher.isFrameScheduled, isTrue);
    frames.pumpFrame();
    expect(frames.hasScheduledFrame, isFalse);
    expect(stitcher.framesApplied, 1);
  });

  test('keeps only the newest update per key', () {
    stitcher
      ..enqueue(update('old', key: 2))
      ..enqueue(update('new', key: 2));
    expect(log, <String>['superseded old']);
    frames.pumpFrame();
    expect(log, <String>['superseded old', 'apply new', 'after new']);
    expect(stitcher.updatesCoalesced, 1);
  });

  test('drops the oldest unkeyed update when full', () {
    stitcher.enqueue(update('k0', key: 0));
    for (var i = 0; i < 5; i++) {
      stitcher.enqueue(update('n$i'));
    }
    stitcher.enqueue(update('n5'));
    expect(log, <String>['superseded n0']);
    expect(stitcher.updatesDropped, 1);
    frames.pumpFrame();
    expect(log.where((e) => e.startsWith('apply')), <String>[
      'apply k0',
      'apply n1',
      'apply n2',
      'apply n3',
      'apply n4',
      'apply n5',
    ]);
  });

  test('a throwing update is reported and does not stop the batch', () {
    final reported = <FlutterErrorDetails>[];
    final previous = FlutterError.onError;
    FlutterError.onError = reported.add;
    addTearDown(() => FlutterError.onError = previous);
    stitcher
      ..enqueue(AetherCallbackUpdate(onApply: () => throw StateError('bad')))
      ..enqueue(update('good'));
    frames.pumpFrame();
    expect(reported.single.exception, isA<StateError>());
    expect(log, contains('apply good'));
  });

  test('updates enqueued during a frame land in the next frame', () {
    stitcher.enqueue(
      AetherCallbackUpdate(
        onApply: () {
          log.add('apply first');
          stitcher.enqueue(update('second'));
        },
      ),
    );
    frames.pumpFrame();
    expect(log, <String>['apply first']);
    frames.pumpFrame();
    expect(log, <String>['apply first', 'apply second', 'after second']);
  });

  test('dispose supersedes pending updates and ignores later ones', () {
    stitcher
      ..enqueue(update('a'))
      ..dispose()
      ..enqueue(update('b'));
    expect(log, <String>['superseded a', 'superseded b']);
  });

  test('rejects a pending limit that keyed updates could fill', () {
    expect(
      () => AetherStateStitcher(keySpace: 4, maxPending: 4, scheduler: frames),
      throwsArgumentError,
    );
  });

  test('AetherSlotNotifier publishes views and deactivates once', () {
    final first = Float32List(4);
    final second = Float32List(4);
    const payload = AetherPayload(
      taskId: 1,
      slotIndex: 0,
      nodeCount: 1,
      nodeStride: 4,
    );
    final notifier = AetherSlotNotifier(first, payload);
    var notified = 0;
    notifier.addListener(() => notified++);
    notifier.publish(second, payload);
    expect(identical(notifier.value, second), isTrue);
    expect(notifier.version, 1);
    notifier
      ..deactivate()
      ..deactivate();
    expect(notifier.isActive, isFalse);
    expect(notified, 2);
  });
}
