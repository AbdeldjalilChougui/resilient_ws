import 'dart:math';

/// Exponential reconnect delays with jitter and a cap.
///
/// The un-jittered delay for attempt `n` (0-based) is
/// `min(max, initial * multiplier^n)`. Jitter then removes a random share of
/// up to [jitter] of it, so the result lies in
/// `[base * (1 - jitter), base]`. Jitter only ever shortens a delay, so [max]
/// is a hard cap, and clients that lost their connection at the same moment
/// do not all come back at the same moment.
class Backoff {
  /// Creates a backoff policy.
  ///
  /// Throws [ArgumentError] if [initial] is negative or greater than [max],
  /// if [multiplier] is below 1, or if [jitter] is outside `0..1`.
  Backoff({
    this.initial = const Duration(seconds: 1),
    this.max = const Duration(seconds: 30),
    this.multiplier = 2,
    this.jitter = 0.2,
    Random? random,
  }) : _random = random ?? Random() {
    if (initial.isNegative) {
      throw ArgumentError.value(initial, 'initial', 'must not be negative');
    }
    if (initial > max) {
      throw ArgumentError.value(max, 'max', 'must be >= initial');
    }
    if (multiplier < 1) {
      throw ArgumentError.value(multiplier, 'multiplier', 'must be >= 1');
    }
    if (jitter < 0 || jitter > 1) {
      throw ArgumentError.value(jitter, 'jitter', 'must be within 0..1');
    }
  }

  /// Delay before the first retry.
  final Duration initial;

  /// Upper bound for any delay.
  final Duration max;

  /// Growth factor between consecutive attempts.
  final double multiplier;

  /// Largest share of a delay, from 0 to 1, that jitter may remove.
  final double jitter;

  final Random _random;

  /// The delay before retry number [attempt] (0 for the first retry).
  Duration delay(int attempt) {
    final maxUs = max.inMicroseconds;
    var baseUs = initial.inMicroseconds.toDouble();
    for (var i = 0; i < attempt && baseUs < maxUs; i++) {
      baseUs *= multiplier;
    }
    if (baseUs > maxUs) baseUs = maxUs.toDouble();
    final jittered = baseUs * (1 - jitter * _random.nextDouble());
    return Duration(microseconds: jittered.round());
  }
}
