import 'dart:convert';

/// Converts between typed messages and WebSocket frames.
abstract interface class MessageCodec<T> {
  /// Encodes [message] into a frame: a `String` or a `List<int>`.
  Object encode(T message);

  /// Decodes an inbound [frame] (a `String` or a `List<int>`).
  ///
  /// Throwing reports the error on `errors`; the frame is skipped.
  T decode(Object? frame);
}

/// A [MessageCodec] for JSON text frames.
///
/// Without arguments it passes decoded JSON values through, typed as [T].
class JsonMessageCodec<T> implements MessageCodec<T> {
  /// Creates a JSON codec. [fromJson] builds a [T] from a decoded JSON value;
  /// [toJson] turns a [T] into something `jsonEncode` accepts.
  const JsonMessageCodec({
    T Function(Object? json)? fromJson,
    Object? Function(T message)? toJson,
  })  : _fromJson = fromJson,
        _toJson = toJson;

  final T Function(Object? json)? _fromJson;
  final Object? Function(T message)? _toJson;

  @override
  Object encode(T message) =>
      jsonEncode(_toJson == null ? message : _toJson(message));

  @override
  T decode(Object? frame) {
    final text = frame is List<int> ? utf8.decode(frame) : frame as String;
    final json = jsonDecode(text);
    final fromJson = _fromJson;
    return fromJson == null ? json as T : fromJson(json);
  }
}

/// Builds the frames that subscribe to and unsubscribe from a topic.
abstract interface class SubscriptionCodec {
  /// Default: `{"op":"sub","topic":<topic>}` and
  /// `{"op":"unsub","topic":<topic>}`.
  const factory SubscriptionCodec.json({
    String opKey,
    String topicKey,
    String subscribeOp,
    String unsubscribeOp,
  }) = JsonSubscriptionCodec;

  /// A codec from two functions.
  factory SubscriptionCodec.from({
    required Object Function(String topic) subscribe,
    required Object Function(String topic) unsubscribe,
  }) = _FunctionSubscriptionCodec;

  /// Frame that subscribes to [topic].
  Object subscribe(String topic);

  /// Frame that unsubscribes from [topic].
  Object unsubscribe(String topic);
}

/// JSON subscription frames with configurable keys and op names.
class JsonSubscriptionCodec implements SubscriptionCodec {
  /// Creates the codec. Defaults produce `{"op":"sub","topic":"x"}`.
  const JsonSubscriptionCodec({
    this.opKey = 'op',
    this.topicKey = 'topic',
    this.subscribeOp = 'sub',
    this.unsubscribeOp = 'unsub',
  });

  /// Key holding the operation.
  final String opKey;

  /// Key holding the topic.
  final String topicKey;

  /// Operation value for subscribing.
  final String subscribeOp;

  /// Operation value for unsubscribing.
  final String unsubscribeOp;

  @override
  Object subscribe(String topic) =>
      jsonEncode({opKey: subscribeOp, topicKey: topic});

  @override
  Object unsubscribe(String topic) =>
      jsonEncode({opKey: unsubscribeOp, topicKey: topic});
}

class _FunctionSubscriptionCodec implements SubscriptionCodec {
  _FunctionSubscriptionCodec({
    required Object Function(String topic) subscribe,
    required Object Function(String topic) unsubscribe,
  })  : _sub = subscribe,
        _unsub = unsubscribe;

  final Object Function(String) _sub;
  final Object Function(String) _unsub;

  @override
  Object subscribe(String topic) => _sub(topic);

  @override
  Object unsubscribe(String topic) => _unsub(topic);
}
