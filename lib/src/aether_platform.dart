// Picks the backend for the current platform: isolates and native shared
// memory everywhere dart:ffi exists, the UI-thread backend on the web.

export 'aether_memory_grid.dart'
    if (dart.library.js_interop) 'aether_memory_grid_web.dart';
export 'aether_one_shot.dart'
    if (dart.library.js_interop) 'aether_one_shot_web.dart';
export 'aether_pool_manager.dart'
    if (dart.library.js_interop) 'aether_pool_web.dart';
