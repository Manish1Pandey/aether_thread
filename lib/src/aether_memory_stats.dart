/// Counts the slot memory currently held by this isolate, on every
/// platform, so apps and tests can check that nothing leaks.
abstract final class AetherMemoryStats {
  static int _liveAllocations = 0;
  static int _liveBytes = 0;

  /// Memory blocks allocated by this isolate and not yet freed.
  static int get liveAllocations => _liveAllocations;

  /// Bytes allocated by this isolate and not yet freed.
  static int get liveBytes => _liveBytes;

  /// Records an allocation. Internal to the memory backends.
  static void recordAllocation(int bytes) {
    _liveAllocations++;
    _liveBytes += bytes;
  }

  /// Records a release. Internal to the memory backends.
  static void recordRelease(int bytes) {
    _liveAllocations--;
    _liveBytes -= bytes;
  }
}
