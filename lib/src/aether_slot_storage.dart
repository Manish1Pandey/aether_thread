// Slot bookkeeping shared by both memory backends: geometry, offset math,
// node and header access, cached slot views and the slot free-list. Each
// backend only supplies the memory (native on io, heap on the web).

import 'dart:typed_data';

import 'aether_payload.dart';

/// The memory-independent part of a slot buffer. Use
/// `ContiguousMemoryBuffer`, which extends it for the current platform.
///
/// This class cannot implement `Finalizable` (that lives in `dart:ffi`,
/// which the web build cannot import), so the VM's keep-alive guarantee does
/// not cover these method bodies. The native buffer is always held strongly
/// by its runtime and pool while in use, which is what keeps it alive.
abstract base class AetherSlotStorage {
  /// Wraps [headers] and [floats], which must hold `slotCount ×
  /// AetherPayload.rowWords` words and `slotCount × slotStrideFloats`
  /// floats. [release] runs once, on the first [dispose] of an owner.
  AetherSlotStorage(
    this.descriptor,
    this._owner,
    this.headers,
    this._floats, {
    void Function(AetherSlotStorage self)? release,
  }) : _release = release {
    _slotViews = List<Float32List>.generate(
      descriptor.slotCount,
      (slot) => Float32List.sublistView(
        _floats,
        slot * descriptor.slotStrideFloats,
        (slot + 1) * descriptor.slotStrideFloats,
      ),
      growable: false,
    );
    if (_owner) {
      _freeStack = Int32List(descriptor.slotCount);
      _inUse = Uint8List(descriptor.slotCount);
      for (var i = 0; i < descriptor.slotCount; i++) {
        // Highest index at the bottom, so slot 0 is handed out first.
        _freeStack[i] = descriptor.slotCount - 1 - i;
      }
      _freeTop = descriptor.slotCount;
    } else {
      _freeStack = Int32List(0);
      _inUse = Uint8List(0);
    }
  }

  /// Validates the geometry and returns the descriptor for it, with the
  /// stride rounded by [roundStride] and the given [address].
  static AetherBufferDescriptor plan({
    required int slotCount,
    required int slotStrideFloats,
    required int nodeStride,
    int address = 0,
  }) {
    if (slotCount < 1) {
      throw RangeError.range(slotCount, 1, null, 'slotCount');
    }
    if (nodeStride < 1) {
      throw RangeError.range(nodeStride, 1, null, 'nodeStride');
    }
    if (slotStrideFloats < nodeStride) {
      throw RangeError.range(
        slotStrideFloats,
        nodeStride,
        null,
        'slotStrideFloats',
      );
    }
    return AetherBufferDescriptor(
      address: address,
      slotCount: slotCount,
      slotStrideFloats: roundStride(slotStrideFloats, nodeStride),
      nodeStride: nodeStride,
    );
  }

  /// Rounds [requested] up so it is a multiple of both 16 and [nodeStride].
  static int roundStride(int requested, int nodeStride) {
    final unit = _lcm(16, nodeStride);
    return ((requested + unit - 1) ~/ unit) * unit;
  }

  static int _gcd(int a, int b) => b == 0 ? a : _gcd(b, a % b);
  static int _lcm(int a, int b) => a ~/ _gcd(a, b) * b;

  /// Address, slot geometry and stride.
  final AetherBufferDescriptor descriptor;

  /// Header rows, [AetherPayload.rowWords] words per slot.
  final List<int> headers;

  final bool _owner;
  final Float32List _floats;
  final void Function(AetherSlotStorage self)? _release;
  late final List<Float32List> _slotViews;
  late final Int32List _freeStack;
  late final Uint8List _inUse;
  int _freeTop = 0;
  bool _disposed = false;

  /// Number of slots.
  int get slotCount => descriptor.slotCount;

  /// Floats per slot.
  int get slotStrideFloats => descriptor.slotStrideFloats;

  /// Floats per node.
  int get nodeStride => descriptor.nodeStride;

  /// Nodes that fit in one slot.
  int get nodesPerSlot => descriptor.slotStrideFloats ~/ descriptor.nodeStride;

  /// Whether this isolate owns (and will free) the memory.
  bool get isOwner => _owner;

  /// Whether [dispose] has run.
  bool get isDisposed => _disposed;

  /// Slots not currently handed out. Owner only.
  int get freeSlotCount => _freeTop;

  void _checkAlive() {
    if (_disposed) {
      throw StateError('Slot memory used after dispose()');
    }
  }

  void _checkSlot(int slot) {
    if (slot < 0 || slot >= descriptor.slotCount) {
      throw RangeError.range(slot, 0, descriptor.slotCount - 1, 'slot');
    }
  }

  void _checkNode(int node) {
    if (node < 0 || node >= nodesPerSlot) {
      throw RangeError.range(node, 0, nodesPerSlot - 1, 'node');
    }
  }

  void _checkField(int field) {
    if (field < 0 || field >= descriptor.nodeStride) {
      throw RangeError.range(field, 0, descriptor.nodeStride - 1, 'field');
    }
  }

  /// Index into the float region of [field] of [node] in [slot].
  int floatIndexOf(int slot, int node, int field) {
    _checkSlot(slot);
    _checkNode(node);
    _checkField(field);
    return slot * descriptor.slotStrideFloats +
        node * descriptor.nodeStride +
        field;
  }

  /// Byte offset from the start of the float region:
  /// `((slot × slotStride) + (node × nodeStride) + field) × 4`.
  int byteOffsetOf(int slot, int node, int field) =>
      floatIndexOf(slot, node, field) * Float32List.bytesPerElement;

  /// Absolute native address of [field] of [node] in [slot].
  int addressOf(int slot, int node, int field) =>
      descriptor.address +
      descriptor.headerBytes +
      byteOffsetOf(slot, node, field);

  /// Reads one float.
  double readField(int slot, int node, int field) {
    _checkAlive();
    return _floats[floatIndexOf(slot, node, field)];
  }

  /// Writes one float.
  void writeField(int slot, int node, int field, double value) {
    _checkAlive();
    _floats[floatIndexOf(slot, node, field)] = value;
  }

  /// Copies node [node] of [slot] into [out], which must hold at least
  /// [nodeStride] floats. Allocates nothing.
  void readNode(int slot, int node, Float32List out) {
    _checkAlive();
    if (out.length < descriptor.nodeStride) {
      throw ArgumentError.value(
        out.length,
        'out.length',
        'must be at least nodeStride (${descriptor.nodeStride})',
      );
    }
    final start = floatIndexOf(slot, node, 0);
    out.setRange(0, descriptor.nodeStride, _floats, start);
  }

  /// Copies [nodeStride] floats from [source] into node [node] of [slot].
  /// Allocates nothing.
  void writeNode(int slot, int node, Float32List source) {
    _checkAlive();
    if (source.length < descriptor.nodeStride) {
      throw ArgumentError.value(
        source.length,
        'source.length',
        'must be at least nodeStride (${descriptor.nodeStride})',
      );
    }
    final start = floatIndexOf(slot, node, 0);
    _floats.setRange(start, start + descriptor.nodeStride, source);
  }

  /// The whole of [slot] as a `Float32List` over shared memory. The same
  /// view object is returned every time.
  Float32List slotView(int slot) {
    _checkAlive();
    _checkSlot(slot);
    return _slotViews[slot];
  }

  /// Sets every float of [slot] to zero and clears its header row.
  void clearSlot(int slot) {
    _checkAlive();
    _checkSlot(slot);
    _slotViews[slot].fillRange(0, descriptor.slotStrideFloats, 0);
    final base = slot * AetherPayload.rowWords;
    headers.fillRange(base, base + AetherPayload.rowWords, 0);
  }

  /// Writes [payload] into its slot's header row.
  void writeHeader(AetherPayload payload) {
    _checkAlive();
    _checkSlot(payload.slotIndex);
    payload.packInto(headers, payload.slotIndex);
  }

  /// Reads the header row of [slot].
  AetherPayload readHeader(int slot) {
    _checkAlive();
    _checkSlot(slot);
    return AetherPayload.unpackFrom(headers, slot);
  }

  /// Takes a free slot, or returns -1 when none is free. Owner only.
  int acquireSlot() {
    _checkAlive();
    _checkOwner();
    if (_freeTop == 0) {
      return -1;
    }
    final slot = _freeStack[--_freeTop];
    _inUse[slot] = 1;
    return slot;
  }

  /// Returns [slot] to the free list. Owner only.
  ///
  /// Throws [StateError] if the slot is not currently acquired.
  void releaseSlot(int slot) {
    _checkAlive();
    _checkOwner();
    _checkSlot(slot);
    if (_inUse[slot] == 0) {
      throw StateError('Slot $slot released twice');
    }
    _inUse[slot] = 0;
    _freeStack[_freeTop++] = slot;
  }

  /// Whether [slot] is currently acquired. Owner only.
  bool isSlotInUse(int slot) {
    _checkOwner();
    _checkSlot(slot);
    return _inUse[slot] == 1;
  }

  void _checkOwner() {
    if (!_owner) {
      throw StateError('Only the owning isolate can manage slots');
    }
  }

  /// Releases the memory (owner) or drops the views (attached).
  ///
  /// The caller must make sure no worker still touches the memory; the pool
  /// does this by waiting for every worker to exit first. Views returned by
  /// [slotView] (and any typed views made from them) point at released
  /// memory afterwards and must not be used: only this object's methods
  /// check for disposal.
  void dispose() {
    if (_disposed) {
      return;
    }
    _disposed = true;
    if (_owner) {
      _release?.call(this);
    }
  }
}
