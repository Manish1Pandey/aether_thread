@TestOn('vm')
library;

import 'dart:isolate';
import 'dart:typed_data';

import 'package:aether_thread/aether_thread.dart';
import 'package:flutter_test/flutter_test.dart';

import 'support.dart';

void main() {
  group('AetherPayload', () {
    test('packs into and unpacks from a header row', () {
      const payload = AetherPayload(
        taskId: 9001,
        slotIndex: 3,
        nodeCount: 120,
        nodeStride: 4,
        priority: AetherPriority.high,
      );
      final headers = Int64List(AetherPayload.rowWords * 5);
      payload.packInto(headers, 3);
      expect(AetherPayload.unpackFrom(headers, 3), payload);
      expect(headers.sublist(0, AetherPayload.rowWords * 3), everyElement(0));
      expect(payload.floatCount, 480);
    });
  });

  group('ContiguousMemoryBuffer', () {
    late ContiguousMemoryBuffer buffer;

    setUp(() {
      buffer = ContiguousMemoryBuffer.allocate(
        slotCount: 4,
        slotStrideFloats: 100,
        nodeStride: 4,
      );
    });

    tearDown(() => buffer.dispose());

    test('rounds the stride to a cache line and the node stride', () {
      expect(buffer.slotStrideFloats, 112);
      expect(ContiguousMemoryBuffer.roundStride(100, 3), 144);
      expect(ContiguousMemoryBuffer.roundStride(16, 4), 16);
      expect(buffer.nodesPerSlot, 28);
    });

    test('computes byte offsets with the documented formula', () {
      expect(buffer.byteOffsetOf(0, 0, 0), 0);
      expect(buffer.byteOffsetOf(2, 5, 3), ((2 * 112) + (5 * 4) + 3) * 4);
      expect(
        buffer.addressOf(1, 0, 0),
        buffer.descriptor.address + buffer.descriptor.headerBytes + 112 * 4,
      );
    });

    test('rejects out-of-range slot, node and field', () {
      expect(() => buffer.byteOffsetOf(4, 0, 0), throwsRangeError);
      expect(() => buffer.byteOffsetOf(0, 28, 0), throwsRangeError);
      expect(() => buffer.byteOffsetOf(0, 0, 4), throwsRangeError);
      expect(() => buffer.byteOffsetOf(-1, 0, 0), throwsRangeError);
    });

    test('starts zeroed and reads back node writes without allocating', () {
      expect(buffer.slotView(0), everyElement(0));
      final node = Float32List.fromList(<double>[1, 2, 3, 4]);
      buffer.writeNode(1, 7, node);
      final out = Float32List(4);
      buffer.readNode(1, 7, out);
      expect(out, <double>[1, 2, 3, 4]);
      expect(buffer.readField(1, 7, 2), 3);
      expect(buffer.slotView(1)[7 * 4 + 3], 4);
      expect(buffer.slotView(0), everyElement(0));
    });

    test('returns the same cached view every time', () {
      expect(identical(buffer.slotView(2), buffer.slotView(2)), isTrue);
    });

    test('hands out and takes back slots, and rejects double release', () {
      final a = buffer.acquireSlot();
      final b = buffer.acquireSlot();
      expect(<int>{a, b}, hasLength(2));
      expect(buffer.freeSlotCount, 2);
      buffer.releaseSlot(a);
      expect(() => buffer.releaseSlot(a), throwsStateError);
      buffer
        ..acquireSlot()
        ..acquireSlot()
        ..acquireSlot();
      expect(buffer.acquireSlot(), -1);
    });

    test('clearSlot zeroes data and header', () {
      buffer.slotView(1).fillRange(0, 10, 5);
      buffer.writeHeader(
        const AetherPayload(
          taskId: 1,
          slotIndex: 1,
          nodeCount: 2,
          nodeStride: 4,
        ),
      );
      buffer.clearSlot(1);
      expect(buffer.slotView(1), everyElement(0));
      expect(buffer.readHeader(1).taskId, 0);
    });

    test('a write on another isolate is visible here without a copy', () async {
      final descriptor = buffer.descriptor;
      final payload = const AetherPayload(
        taskId: 77,
        slotIndex: 2,
        nodeCount: 3,
        nodeStride: 4,
      );
      buffer.writeHeader(payload);
      final seenTaskId = await Isolate.run(attachAndFillTask(descriptor));
      expect(seenTaskId, 77);
      expect(buffer.slotView(2).sublist(0, 12), <double>[
        for (var i = 0; i < 12; i++) i * 1.5,
      ]);
    });

    test('attached buffers cannot manage slots', () {
      final attached = ContiguousMemoryBuffer.attach(buffer.descriptor);
      expect(attached.acquireSlot, throwsStateError);
      attached.dispose();
      expect(buffer.isDisposed, isFalse);
    });
  });

  test('dispose frees the memory and blocks further use', () {
    final before = AetherMemoryStats.liveAllocations;
    final buffer = ContiguousMemoryBuffer.allocate(
      slotCount: 2,
      slotStrideFloats: 16,
      nodeStride: 2,
    );
    expect(AetherMemoryStats.liveAllocations, before + 1);
    buffer.dispose();
    expect(AetherMemoryStats.liveAllocations, before);
    expect(() => buffer.slotView(0), throwsStateError);
    buffer.dispose();
  });

  test('rejects invalid geometry', () {
    expect(
      () => ContiguousMemoryBuffer.allocate(
        slotCount: 0,
        slotStrideFloats: 16,
        nodeStride: 4,
      ),
      throwsRangeError,
    );
    expect(
      () => ContiguousMemoryBuffer.allocate(
        slotCount: 1,
        slotStrideFloats: 2,
        nodeStride: 4,
      ),
      throwsRangeError,
    );
  });
}
