// Platform-neutral types of the memory grid: priorities, the payload header,
// the buffer descriptor and typed views over a slot. Shared by the native
// (dart:ffi) and web (heap) backends.

import 'dart:typed_data';

/// Scheduling priority of a task. Higher runs first.
enum AetherPriority {
  /// Background work that can wait.
  low,

  /// The default.
  normal,

  /// Latency-sensitive work, taken before everything else.
  high,
}

/// Immutable header describing one unit of grid work.
///
/// It is packed into its slot's header row in shared memory, so the worker
/// reads the task description from the same place as the data.
final class AetherPayload {
  /// Creates a payload header.
  const AetherPayload({
    required this.taskId,
    required this.slotIndex,
    required this.nodeCount,
    required this.nodeStride,
    this.priority = AetherPriority.normal,
  });

  /// Number of `Int64` words used in a header row.
  static const int usedWords = 5;

  /// Number of `Int64` words reserved per header row (one cache line).
  static const int rowWords = 8;

  /// Id of the task that owns the slot.
  final int taskId;

  /// Slot the data lives in.
  final int slotIndex;

  /// Number of nodes the task works on.
  final int nodeCount;

  /// Floats per node.
  final int nodeStride;

  /// Priority the task was submitted with.
  final AetherPriority priority;

  /// Number of floats in use: `nodeCount × nodeStride`.
  int get floatCount => nodeCount * nodeStride;

  /// Writes this payload into header [row] of [headers].
  void packInto(List<int> headers, int row) {
    final base = row * rowWords;
    headers[base] = taskId;
    headers[base + 1] = slotIndex;
    headers[base + 2] = nodeCount;
    headers[base + 3] = nodeStride;
    headers[base + 4] = priority.index;
  }

  /// Reads the payload stored in header [row] of [headers].
  static AetherPayload unpackFrom(List<int> headers, int row) {
    final base = row * rowWords;
    return AetherPayload(
      taskId: headers[base],
      slotIndex: headers[base + 1],
      nodeCount: headers[base + 2],
      nodeStride: headers[base + 3],
      priority: AetherPriority.values[headers[base + 4]],
    );
  }

  @override
  bool operator ==(Object other) =>
      other is AetherPayload &&
      other.taskId == taskId &&
      other.slotIndex == slotIndex &&
      other.nodeCount == nodeCount &&
      other.nodeStride == nodeStride &&
      other.priority == priority;

  @override
  int get hashCode =>
      Object.hash(taskId, slotIndex, nodeCount, nodeStride, priority);

  @override
  String toString() =>
      'AetherPayload(task $taskId, slot $slotIndex, '
      '$nodeCount × $nodeStride, ${priority.name})';
}

/// Everything a worker needs to attach to a buffer. Plain integers, so it
/// crosses isolates for free. (On the web, where there is no shared memory,
/// [address] is 0.)
final class AetherBufferDescriptor {
  /// Creates a descriptor.
  const AetherBufferDescriptor({
    required this.address,
    required this.slotCount,
    required this.slotStrideFloats,
    required this.nodeStride,
  });

  /// Native address of the block.
  final int address;

  /// Number of slots.
  final int slotCount;

  /// Floats per slot (already rounded to a cache line).
  final int slotStrideFloats;

  /// Floats per node.
  final int nodeStride;

  /// Bytes taken by the header region.
  int get headerBytes => slotCount * AetherPayload.rowWords * 8;

  /// Total bytes of the block.
  int get totalBytes => headerBytes + slotCount * slotStrideFloats * 4;
}

/// Other views over the same shared memory as a slot's `Float32List`.
///
/// Nothing is copied: each view reads and writes the very same bytes. Slots
/// start on 64-byte boundaries, which satisfies every view's alignment.
extension AetherSlotViews on Float32List {
  /// Four floats per element, for SIMD arithmetic. Covers the first
  /// `length ~/ 4 × 4` floats.
  Float32x4List asFloat32x4() =>
      Float32x4List.view(buffer, offsetInBytes, length ~/ 4);

  /// The same bytes as 32-bit integers (one per float).
  Int32List asInt32() => Int32List.view(buffer, offsetInBytes, length);

  /// The same bytes as 32-bit unsigned integers (one per float).
  Uint32List asUint32() => Uint32List.view(buffer, offsetInBytes, length);

  /// The same bytes as 64-bit floats (one per two floats).
  Float64List asFloat64() =>
      Float64List.view(buffer, offsetInBytes, length ~/ 2);

  /// The same bytes, one element per byte.
  Uint8List asBytes() => Uint8List.view(
    buffer,
    offsetInBytes,
    length * Float32List.bytesPerElement,
  );
}
