import 'dart:convert';

/// Application-level ping/pong settings.
///
/// Every [interval] the client sends [ping]. If no frame at all arrives
/// within [timeout] after a ping, the link is considered dead and the client
/// reconnects. Any inbound frame counts as proof of life, not only a pong,
/// so a busy connection never times out. Frames matched by [isPong] are
/// consumed and not delivered to `messages`.
class Heartbeat {
  /// Creates heartbeat settings. The defaults send `{"type":"ping"}` and
  /// expect `{"type":"pong"}`.
  const Heartbeat({
    this.interval = const Duration(seconds: 25),
    this.timeout = const Duration(seconds: 10),
    this.ping = defaultPing,
    this.isPong = defaultIsPong,
  });

  /// Time between pings.
  final Duration interval;

  /// How long to wait for any frame after a ping.
  final Duration timeout;

  /// Builds the ping frame (a `String` or `List<int>`).
  final Object Function() ping;

  /// Whether an inbound frame is a pong.
  final bool Function(Object? frame) isPong;

  /// Default ping frame: `{"type":"ping"}`.
  static Object defaultPing() => '{"type":"ping"}';

  /// Default pong test: a JSON object whose `type` is `pong`.
  static bool defaultIsPong(Object? frame) {
    if (frame is! String || !frame.contains('pong')) return false;
    try {
      final decoded = jsonDecode(frame);
      return decoded is Map && decoded['type'] == 'pong';
    } on FormatException {
      return false;
    }
  }
}
