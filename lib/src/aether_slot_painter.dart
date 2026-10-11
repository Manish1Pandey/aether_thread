// Paints points straight out of a shared memory slot.

import 'dart:typed_data';
import 'dart:ui' show PointMode;

import 'package:flutter/rendering.dart';

import 'aether_state_stitcher.dart';

/// Draws the first [pointCount] (x, y) pairs of a slot with
/// `Canvas.drawRawPoints`.
///
/// The slot's native-backed `Float32List` goes to the engine as is: no Dart
/// copy is made. (The engine itself records the points into its display
/// list.) Coordinates are in unit space, `0..1` on both axes, mapped onto
/// the largest centred square that fits the canvas. Repaints whenever
/// [notifier] publishes a step.
class AetherSlotPainter extends CustomPainter {
  /// Creates a painter for [notifier].
  AetherSlotPainter({
    required this.notifier,
    required this.pointCount,
    required this.color,
    this.pointSize = 1.5,
    this.pointMode = PointMode.points,
  }) : super(repaint: notifier);

  /// The slot to draw.
  final AetherSlotNotifier notifier;

  /// Number of (x, y) pairs read from the start of the slot.
  final int pointCount;

  /// Point color.
  final Color color;

  /// Point diameter in logical pixels.
  final double pointSize;

  /// How the points are joined.
  final PointMode pointMode;

  final Paint _paint = Paint()..strokeCap = StrokeCap.round;
  Float32List? _source;
  Float32List? _points;

  @override
  void paint(Canvas canvas, Size size) {
    if (!notifier.isActive || pointCount <= 0) {
      return;
    }
    final side = size.shortestSide;
    if (side <= 0) {
      return;
    }
    final data = notifier.value;
    if (!identical(data, _source)) {
      _source = data;
      _points = Float32List.sublistView(data, 0, pointCount * 2);
    }
    _paint
      ..color = color
      ..strokeWidth = pointSize / side;
    canvas
      ..save()
      ..translate((size.width - side) / 2, (size.height - side) / 2)
      ..scale(side)
      ..drawRawPoints(pointMode, _points!, _paint)
      ..restore();
  }

  @override
  bool shouldRepaint(AetherSlotPainter oldDelegate) =>
      !identical(oldDelegate.notifier, notifier) ||
      oldDelegate.pointCount != pointCount ||
      oldDelegate.color != color ||
      oldDelegate.pointSize != pointSize ||
      oldDelegate.pointMode != pointMode;
}
