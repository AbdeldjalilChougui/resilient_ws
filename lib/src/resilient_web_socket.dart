import 'dart:async';
import 'dart:collection';

import 'package:web_socket_channel/web_socket_channel.dart';

import 'backoff.dart';
import 'codecs.dart';
import 'connection_status.dart';
import 'connector.dart';
import 'connector_web.dart' if (dart.library.io) 'connector_io.dart';
import 'exceptions.dart';
import 'heartbeat.dart';
import 'outbox.dart';
import 'token_auth.dart';

/// A WebSocket client that keeps itself connected on unreliable networks.
///
/// * Reconnects with exponential [Backoff] and jitter; the backoff resets
///   once a connection has stayed open for [stableAfter].
/// * Calls the token provider before **every** attempt, so reconnects never
///   reuse an expired token.
/// * Optional application-level [Heartbeat] detects dead links that TCP
///   has not noticed yet.
/// * Messages sent while disconnected wait in a bounded buffer and are
///   flushed, in order, once the connection is back.
/// * Topic subscriptions are remembered and re-sent after each reconnect.
/// * Close codes in [permanentCloseCodes] stop the client instead of
///   retrying, and [close] never triggers a reconnect.
/// * An optional `networkAvailable` stream pauses reconnect attempts while
///   the device is offline and retries at once when it comes back.
///
/// On connect the client sends, in this order: the first-message token (if
/// [TokenAuth.firstMessage] is used), the subscriptions, then the buffer.
class ResilientWebSocket<T> {
  /// Creates a client for [url]. Nothing happens until [connect] is called.
  ///
  /// * [codec] turns messages of type [T] into frames and back.
  /// * [tokenProvider] is called before every connection attempt; [tokenAuth]
  ///   says where the token goes. A null token connects without one. If the
  ///   provider throws, the attempt counts as failed and is retried.
  /// * [backoff] defaults to 1 s doubling up to 30 s with 20 % jitter.
  /// * [connectTimeout] bounds the handshake.
  /// * [outboxCapacity] and [overflowPolicy] bound the send buffer;
  ///   [onDrop] sees each message the buffer discards.
  /// * [subscriptionCodec] builds subscribe and unsubscribe frames.
  /// * [maxAttempts], if set, stops the client after that many consecutive
  ///   failed attempts.
  /// * [headers] are sent on the upgrade request (not on the web).
  /// * [connector] replaces the platform default, mainly for tests.
  ResilientWebSocket({
    required this.url,
    required MessageCodec<T> codec,
    TokenProvider? tokenProvider,
    this.tokenAuth = const TokenAuth.queryParameter(),
    Backoff? backoff,
    this.stableAfter = const Duration(seconds: 10),
    this.connectTimeout = const Duration(seconds: 10),
    this.heartbeat,
    int outboxCapacity = 100,
    OverflowPolicy overflowPolicy = OverflowPolicy.dropOldest,
    void Function(T message)? onDrop,
    SubscriptionCodec subscriptionCodec = const JsonSubscriptionCodec(),
    Set<int> permanentCloseCodes = defaultPermanentCloseCodes,
    Stream<bool>? networkAvailable,
    this.maxAttempts,
    Map<String, String> headers = const {},
    Iterable<String>? protocols,
    WebSocketConnector? connector,
  })  : _codec = codec,
        _tokenProvider = tokenProvider,
        backoff = backoff ?? Backoff(),
        _subscriptionCodec = subscriptionCodec,
        permanentCloseCodes = Set.unmodifiable(permanentCloseCodes),
        _headers = Map.unmodifiable(headers),
        _protocols = protocols,
        _connector = connector ?? platformConnect,
        _outbox = Outbox<(T, Object)>(
          capacity: outboxCapacity,
          policy: overflowPolicy,
          onDrop: onDrop == null ? null : (p) => onDrop(p.$1),
        ) {
    if (maxAttempts != null && maxAttempts! < 1) {
      throw ArgumentError.value(maxAttempts, 'maxAttempts', 'must be >= 1');
    }
    _networkSub = networkAvailable?.listen(_onNetwork);
  }

  /// Close codes treated as permanent unless configured otherwise:
  /// 1008 (policy violation) and the common auth codes 4401 and 4403.
  static const Set<int> defaultPermanentCloseCodes = {1008, 4401, 4403};

  /// The server URL.
  final Uri url;

  /// Where the token goes on each connect.
  final TokenAuth tokenAuth;

  /// Reconnect delays.
  final Backoff backoff;

  /// How long a connection must stay open before the backoff resets.
  final Duration stableAfter;

  /// Maximum time for the WebSocket handshake.
  final Duration connectTimeout;

  /// Heartbeat settings, or null for none.
  final Heartbeat? heartbeat;

  /// Server close codes that stop the client instead of reconnecting.
  final Set<int> permanentCloseCodes;

  /// Consecutive failed attempts after which the client gives up, or null to
  /// retry forever.
  final int? maxAttempts;

  final MessageCodec<T> _codec;
  final TokenProvider? _tokenProvider;
  final SubscriptionCodec _subscriptionCodec;
  final Map<String, String> _headers;
  final Iterable<String>? _protocols;
  final WebSocketConnector _connector;
  final Outbox<(T, Object)> _outbox;
  final LinkedHashSet<String> _topics = LinkedHashSet<String>();

  final StreamController<ConnectionStatus> _states =
      StreamController<ConnectionStatus>.broadcast();
  final StreamController<T> _messages = StreamController<T>.broadcast();
  final StreamController<Object> _errors = StreamController<Object>.broadcast();

  ConnectionStatus _state = ConnectionStatus.idle;
  WebSocketChannel? _channel;
  StreamSubscription<dynamic>? _sub;
  StreamSubscription<bool>? _networkSub;
  Timer? _retryTimer;
  Timer? _stableTimer;
  Timer? _pingTimer;
  Timer? _pongTimer;
  int _gen = 0;
  int _backoffAttempt = 0;
  int _failures = 0;
  bool _inFlight = false;
  bool _networkUp = true;
  int? _closeCode;
  String? _closeReason;

  /// State changes. Broadcast; does not replay the current [state].
  /// Closes after [ConnectionStatus.closed].
  Stream<ConnectionStatus> get states => _states.stream;

  /// The current state.
  ConnectionStatus get state => _state;

  /// Decoded inbound messages. Heartbeat pongs are not included.
  Stream<T> get messages => _messages.stream;

  /// Non-fatal and fatal problems: connection failures, decode errors,
  /// [HeartbeatTimeoutException], [PermanentCloseException] and
  /// [RetryLimitExceededException].
  Stream<Object> get errors => _errors.stream;

  /// Topics currently subscribed to, in subscription order.
  Set<String> get topics => UnmodifiableSetView(_topics);

  /// Number of messages waiting in the send buffer.
  int get bufferedCount => _outbox.length;

  /// The close code that stopped the client, if the server closed it with a
  /// permanent code.
  int? get closeCode => _closeCode;

  /// The close reason that came with [closeCode].
  String? get closeReason => _closeReason;

  /// Starts connecting. Calling it again while running does nothing.
  ///
  /// Throws [StateError] after [close].
  void connect() {
    _checkNotClosed();
    if (_state != ConnectionStatus.idle) return;
    _setState(ConnectionStatus.connecting);
    _startAttempt();
  }

  /// Drops the current connection, if any, and connects again right away,
  /// for example when the app returns to the foreground.
  void reconnect() {
    _checkNotClosed();
    if (_state == ConnectionStatus.idle) return connect();
    _dropChannel();
    _setState(ConnectionStatus.reconnecting);
    _startAttempt();
  }

  /// Sends [message] now if open, otherwise buffers it.
  ///
  /// Encoding errors are thrown to the caller. Throws [StateError] after
  /// the client has closed.
  void send(T message) {
    _checkNotClosed();
    final frame = _codec.encode(message);
    final channel = _channel;
    if (_state == ConnectionStatus.open && channel != null) {
      channel.sink.add(frame);
    } else {
      _outbox.add((message, frame));
    }
  }

  /// Subscribes to [topic] now if open, and after every reconnect.
  void subscribe(String topic) {
    _checkNotClosed();
    if (!_topics.add(topic)) return;
    _sendIfOpen(_subscriptionCodec.subscribe(topic));
  }

  /// Unsubscribes from [topic]; it will not be re-sent on reconnect.
  void unsubscribe(String topic) {
    _checkNotClosed();
    if (!_topics.remove(topic)) return;
    _sendIfOpen(_subscriptionCodec.unsubscribe(topic));
  }

  /// Closes the connection with [code] and [reason] and stops for good.
  ///
  /// No reconnect follows. The [states], [messages] and [errors] streams
  /// close. Safe to call more than once.
  Future<void> close([int code = 1000, String? reason]) async {
    if (_state == ConnectionStatus.closed) return;
    final channel = _channel;
    final sub = _sub;
    _channel = null;
    _sub = null;
    _terminate();
    if (channel != null) {
      try {
        await channel.sink
            .close(code, reason)
            .timeout(const Duration(seconds: 2), onTimeout: () {});
      } on Object {
        // Already gone.
      }
    }
    await sub?.cancel();
  }

  // ---------------------------------------------------------------------

  void _checkNotClosed() {
    if (_state == ConnectionStatus.closed) {
      throw StateError('ResilientWebSocket is closed');
    }
  }

  void _setState(ConnectionStatus s) {
    if (_state == s) return;
    _state = s;
    if (!_states.isClosed) _states.add(s);
  }

  void _sendIfOpen(Object frame) {
    final channel = _channel;
    if (_state == ConnectionStatus.open && channel != null) {
      channel.sink.add(frame);
    }
  }

  void _startAttempt() {
    _retryTimer?.cancel();
    _retryTimer = null;
    if (!_networkUp || _state == ConnectionStatus.closed) return;
    final gen = ++_gen;
    unawaited(_attempt(gen));
  }

  Future<void> _attempt(int gen) async {
    _inFlight = true;
    WebSocketChannel? channel;
    String? token;
    try {
      token = await _tokenProvider?.call();
      if (gen != _gen) return;
      var uri = url;
      final headers = <String, String>{..._headers};
      final auth = tokenAuth;
      if (token != null) {
        switch (auth) {
          case QueryTokenAuth(:final name):
            uri = url.replace(
                queryParameters: {...url.queryParametersAll, name: token});
          case HeaderTokenAuth(:final name, :final prefix):
            headers[name] = '$prefix$token';
          case FirstMessageTokenAuth():
            break;
        }
      }
      channel = _connector(uri, headers: headers, protocols: _protocols);
      _channel = channel;
      await channel.ready.timeout(connectTimeout);
    } on Object catch (e) {
      if (gen != _gen) return;
      _inFlight = false;
      if (channel != null) _closeQuietly(channel);
      _channel = null;
      _errors.add(e);
      _onAttemptFailed();
      return;
    } finally {
      if (gen == _gen) _inFlight = false;
    }
    if (gen != _gen) {
      _closeQuietly(channel);
      return;
    }
    _sub = channel.stream.listen(
      (frame) => _onFrame(gen, frame),
      onError: (Object e) {
        if (gen == _gen) _errors.add(e);
      },
      onDone: () => _onDone(gen, channel!),
    );
    _onOpen(gen, channel, token);
  }

  void _onOpen(int gen, WebSocketChannel channel, String? token) {
    _failures = 0;
    _setState(ConnectionStatus.open);
    _stableTimer = Timer(stableAfter, () => _backoffAttempt = 0);
    final sink = channel.sink;
    final auth = tokenAuth;
    if (auth is FirstMessageTokenAuth && token != null) {
      sink.add(auth.build(token));
    }
    for (final topic in _topics) {
      sink.add(_subscriptionCodec.subscribe(topic));
    }
    for (final (_, frame) in _outbox.drain()) {
      sink.add(frame);
    }
    final hb = heartbeat;
    if (hb != null) {
      _pingTimer = Timer.periodic(hb.interval, (_) {
        if (gen != _gen) return;
        try {
          sink.add(hb.ping());
        } on Object catch (e) {
          _errors.add(e);
        }
        _pongTimer ??= Timer(hb.timeout, () => _onHeartbeatTimeout(gen));
      });
    }
  }

  void _onFrame(int gen, Object? frame) {
    if (gen != _gen) return;
    _pongTimer?.cancel();
    _pongTimer = null;
    final hb = heartbeat;
    if (hb != null && hb.isPong(frame)) return;
    final T message;
    try {
      message = _codec.decode(frame);
    } on Object catch (e) {
      _errors.add(e);
      return;
    }
    _messages.add(message);
  }

  void _onDone(int gen, WebSocketChannel channel) {
    if (gen != _gen || _state == ConnectionStatus.closed) return;
    _stopConnectionTimers();
    _sub = null;
    _channel = null;
    final code = channel.closeCode;
    if (code != null && permanentCloseCodes.contains(code)) {
      _closeCode = code;
      _closeReason = channel.closeReason;
      _errors.add(PermanentCloseException(code, channel.closeReason));
      _terminate();
      return;
    }
    _scheduleReconnect();
  }

  void _onHeartbeatTimeout(int gen) {
    if (gen != _gen) return;
    _errors.add(HeartbeatTimeoutException(heartbeat!.timeout));
    _dropChannel();
    _scheduleReconnect();
  }

  void _onAttemptFailed() {
    _failures++;
    final limit = maxAttempts;
    if (limit != null && _failures >= limit) {
      _errors.add(RetryLimitExceededException(_failures));
      _terminate();
      return;
    }
    _scheduleReconnect();
  }

  void _scheduleReconnect() {
    _setState(ConnectionStatus.reconnecting);
    if (!_networkUp) return;
    _retryTimer?.cancel();
    _retryTimer = Timer(backoff.delay(_backoffAttempt++), _startAttempt);
  }

  void _onNetwork(bool up) {
    if (up == _networkUp) return;
    _networkUp = up;
    if (!up) {
      _retryTimer?.cancel();
      _retryTimer = null;
      return;
    }
    final waiting = _state == ConnectionStatus.connecting ||
        _state == ConnectionStatus.reconnecting;
    if (waiting && !_inFlight && _channel == null) {
      _backoffAttempt = 0;
      _startAttempt();
    }
  }

  /// Invalidates and closes the current channel without triggering onDone.
  void _dropChannel() {
    _gen++;
    _inFlight = false;
    _stopConnectionTimers();
    final sub = _sub;
    final channel = _channel;
    _sub = null;
    _channel = null;
    if (channel != null) _closeQuietly(channel, 4000, 'client reconnect');
    unawaited(sub?.cancel());
  }

  void _closeQuietly(WebSocketChannel channel, [int? code, String? reason]) {
    try {
      unawaited(
          channel.sink.close(code, reason).then<void>((_) {}, onError: (_) {}));
    } on Object {
      // Already closed.
    }
  }

  void _stopConnectionTimers() {
    _stableTimer?.cancel();
    _pingTimer?.cancel();
    _pongTimer?.cancel();
    _stableTimer = _pingTimer = _pongTimer = null;
  }

  /// Enters the terminal state and releases everything but the channel,
  /// which the caller handles.
  void _terminate() {
    _gen++;
    _stopConnectionTimers();
    _retryTimer?.cancel();
    _retryTimer = null;
    unawaited(_networkSub?.cancel());
    _networkSub = null;
    _setState(ConnectionStatus.closed);
    unawaited(_states.close());
    unawaited(_messages.close());
    unawaited(_errors.close());
  }
}
