/// Warm worker-isolate pool for Flutter with shared native memory slots,
/// frame-aligned result application and typed failure handling.
///
/// Start with `AetherConcurrentRunner.initialize()`, then use
/// `computeAsync`, `computeMap`, `computeOnce`, `computeWithProgress`,
/// `computeGrid` or `startContinuous`.
///
/// On the web, where there are no threads that can run Dart closures, the
/// same API runs tasks on the UI thread, one per event-loop turn.
library;

export 'src/aether_concurrent_bridge.dart'
    show
        AetherConcurrentRunner,
        AetherConfig,
        AetherContinuousGrid,
        AetherRuntime,
        AetherSlotCallback,
        AetherStats;
export 'src/aether_failures.dart';
export 'src/aether_memory_stats.dart';
export 'src/aether_platform.dart'
    show AetherOneShotRunner, AetherWorkerPool, ContiguousMemoryBuffer;
export 'src/aether_payload.dart';
export 'src/aether_pool_types.dart'
    show
        AetherGridKernel,
        AetherPoolConfig,
        AetherPoolStats,
        AetherProgressTask,
        AetherStepKernel,
        AetherWorkerInit;
export 'src/aether_slot_painter.dart';
export 'src/aether_state_stitcher.dart';
