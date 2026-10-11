// Web backend of the memory grid. There is no dart:ffi and no shared
// memory on the web, so the slots live in an ordinary heap Float32List. The
// API and slot bookkeeping are the same as the native backend's.

import 'dart:typed_data';

import 'aether_memory_stats.dart';
import 'aether_payload.dart';
import 'aether_slot_storage.dart';

export 'aether_payload.dart';

/// A fixed set of equally sized float slots. On the web they live on the
/// heap of the single Dart thread; there are no other threads to share with.
final class ContiguousMemoryBuffer extends AetherSlotStorage {
  ContiguousMemoryBuffer._(
    AetherBufferDescriptor descriptor,
    List<int> headers,
    Float32List floats,
    int bytes,
  ) : super(
        descriptor,
        true,
        headers,
        floats,
        release: (_) => AetherMemoryStats.recordRelease(bytes),
      );

  /// Allocates a zero-filled buffer of [slotCount] slots, each able to hold
  /// at least [slotStrideFloats] floats, grouped into nodes of [nodeStride].
  ///
  /// [slotStrideFloats] is rounded up to a multiple of 16 floats (64 bytes)
  /// and must be a multiple of [nodeStride] after rounding.
  factory ContiguousMemoryBuffer.allocate({
    required int slotCount,
    required int slotStrideFloats,
    required int nodeStride,
  }) {
    final plan = AetherSlotStorage.plan(
      slotCount: slotCount,
      slotStrideFloats: slotStrideFloats,
      nodeStride: nodeStride,
    );
    // Int64List is not available when compiled to JavaScript; header words
    // (task ids, counts) fit comfortably in a plain List<int>.
    final headers = List<int>.filled(
      plan.slotCount * AetherPayload.rowWords,
      0,
    );
    final floats = Float32List(plan.slotCount * plan.slotStrideFloats);
    final bytes = plan.totalBytes;
    AetherMemoryStats.recordAllocation(bytes);
    return ContiguousMemoryBuffer._(plan, headers, floats, bytes);
  }

  /// Not available on the web: there are no other isolates to share memory
  /// with. Always throws [UnsupportedError].
  factory ContiguousMemoryBuffer.attach(AetherBufferDescriptor descriptor) =>
      throw UnsupportedError(
        'ContiguousMemoryBuffer.attach is not available on the web: there is '
        'no memory shared between threads there.',
      );

  /// Rounds [requested] up so it is a multiple of both 16 and [nodeStride].
  static int roundStride(int requested, int nodeStride) =>
      AetherSlotStorage.roundStride(requested, nodeStride);
}
