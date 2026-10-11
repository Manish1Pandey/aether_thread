import 'dart:io' show Platform;
import 'dart:math' as math;

/// Leaves a core each for Flutter's UI and raster threads.
int aetherDefaultWorkerCount() => math.max(1, Platform.numberOfProcessors - 2);
