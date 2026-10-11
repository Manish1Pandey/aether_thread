// Direct bindings to the platform allocator, so the package needs no
// `package:ffi`. POSIX platforms use libc `calloc`/`free` from the process
// image; Windows uses `CoTaskMemAlloc`/`CoTaskMemFree` from ole32, the same
// allocator `package:ffi` uses there.

import 'dart:ffi';
import 'dart:io' show Platform;

import 'aether_memory_stats.dart';

typedef _CallocNative = Pointer<Void> Function(IntPtr count, IntPtr size);
typedef _CallocDart = Pointer<Void> Function(int count, int size);
typedef _AllocNative = Pointer<Void> Function(IntPtr size);
typedef _AllocDart = Pointer<Void> Function(int size);
typedef _FreeNative = Void Function(Pointer<Void> pointer);
typedef _FreeDart = void Function(Pointer<Void> pointer);

/// Allocates and frees raw native memory for the shared memory grid.
///
/// Every allocation is zero-filled and counted in [AetherMemoryStats].
abstract final class AetherNativeMemory {
  static final DynamicLibrary _library = Platform.isWindows
      ? DynamicLibrary.open('ole32.dll')
      : DynamicLibrary.process();

  static final _CallocDart? _calloc = Platform.isWindows
      ? null
      : _library.lookupFunction<_CallocNative, _CallocDart>('calloc');

  static final _AllocDart? _coTaskMemAlloc = Platform.isWindows
      ? _library.lookupFunction<_AllocNative, _AllocDart>('CoTaskMemAlloc')
      : null;

  static final Pointer<NativeFunction<_FreeNative>> _freePointer = _library
      .lookup<NativeFunction<_FreeNative>>(
        Platform.isWindows ? 'CoTaskMemFree' : 'free',
      );

  static final _FreeDart _free = _freePointer.asFunction<_FreeDart>();

  /// Native function pointer to the matching `free`, for a [NativeFinalizer].
  static Pointer<NativeFinalizerFunction> get freeFunction =>
      _freePointer.cast<NativeFinalizerFunction>();

  /// Allocates [byteCount] zero-filled bytes, 16-byte aligned or better.
  ///
  /// Throws [OutOfMemoryError] when the platform allocator returns null.
  static Pointer<Void> allocateZeroed(int byteCount) {
    if (byteCount <= 0) {
      throw RangeError.range(byteCount, 1, null, 'byteCount');
    }
    final Pointer<Void> pointer;
    final calloc = _calloc;
    if (calloc != null) {
      pointer = calloc(1, byteCount);
    } else {
      pointer = _coTaskMemAlloc!(byteCount);
      if (pointer != nullptr) {
        pointer.cast<Uint8>().asTypedList(byteCount).fillRange(0, byteCount, 0);
      }
    }
    if (pointer == nullptr) {
      throw OutOfMemoryError();
    }
    AetherMemoryStats.recordAllocation(byteCount);
    return pointer;
  }

  /// Frees a block returned by [allocateZeroed] of [byteCount] bytes.
  static void free(Pointer<Void> pointer, int byteCount) {
    _free(pointer);
    AetherMemoryStats.recordRelease(byteCount);
  }
}
