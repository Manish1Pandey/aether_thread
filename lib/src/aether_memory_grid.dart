// MODULE 1 — The zero-copy memory grid.
//
// One native block holds every slot. Its layout is:
//
//   [ header row 0 | header row 1 | ... ][ slot 0 floats | slot 1 floats | ... ]
//     8 × Int64 each (64 bytes)            slotStrideFloats × Float32 each
//
// The block is aligned to 64 bytes, header rows are 64 bytes and slot strides
// are rounded up to a multiple of 16 floats (64 bytes), so every slot starts
// on its own cache line and two workers writing neighbouring slots never
// share a line (no false sharing).
//
// The block lives outside the Dart heap. Each isolate builds its own
// `Float32List` view over the same address, so a write on a worker is the
// same memory the UI isolate reads: nothing is serialized or copied.
//
// Thread safety comes from ownership, not locks: a slot is written by exactly
// one isolate at a time, and ownership is handed over by port messages.

import 'dart:ffi';

import 'aether_native_memory.dart';
import 'aether_payload.dart';
import 'aether_slot_storage.dart';

export 'aether_payload.dart';

/// A fixed set of equally sized float slots in native memory, shared by the
/// main isolate and every worker.
///
/// The main isolate creates it with [ContiguousMemoryBuffer.allocate] and is
/// its owner: only the owner hands out slots and frees the block. Workers
/// use [ContiguousMemoryBuffer.attach], which never frees anything.
final class ContiguousMemoryBuffer extends AetherSlotStorage
    implements Finalizable {
  ContiguousMemoryBuffer._(
    AetherBufferDescriptor descriptor,
    bool owner, {
    super.release,
  }) : super(
         descriptor,
         owner,
         Pointer<Int64>.fromAddress(
           descriptor.address,
         ).asTypedList(descriptor.slotCount * AetherPayload.rowWords),
         Pointer<Float>.fromAddress(
           descriptor.address + descriptor.headerBytes,
         ).asTypedList(descriptor.slotCount * descriptor.slotStrideFloats),
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
    final probe = AetherSlotStorage.plan(
      slotCount: slotCount,
      slotStrideFloats: slotStrideFloats,
      nodeStride: nodeStride,
    );
    // calloc only promises 16-byte alignment; over-allocate and round the
    // base up to a cache line.
    final rawBytes = probe.totalBytes + _cacheLine;
    final pointer = AetherNativeMemory.allocateZeroed(rawBytes);
    final aligned = (pointer.address + _cacheLine - 1) & ~(_cacheLine - 1);
    final buffer = ContiguousMemoryBuffer._(
      AetherBufferDescriptor(
        address: aligned,
        slotCount: probe.slotCount,
        slotStrideFloats: probe.slotStrideFloats,
        nodeStride: probe.nodeStride,
      ),
      true,
      release: (self) {
        _finalizer.detach(self);
        AetherNativeMemory.free(pointer, rawBytes);
      },
    );
    _finalizer.attach(buffer, pointer, detach: buffer, externalSize: rawBytes);
    return buffer;
  }

  /// Attaches to a buffer allocated by another isolate. The result never
  /// frees memory and cannot hand out slots.
  factory ContiguousMemoryBuffer.attach(AetherBufferDescriptor descriptor) =>
      ContiguousMemoryBuffer._(descriptor, false);

  static final NativeFinalizer _finalizer = NativeFinalizer(
    AetherNativeMemory.freeFunction,
  );

  static const int _cacheLine = 64;

  /// Rounds [requested] up so it is a multiple of both 16 and [nodeStride].
  static int roundStride(int requested, int nodeStride) =>
      AetherSlotStorage.roundStride(requested, nodeStride);
}
