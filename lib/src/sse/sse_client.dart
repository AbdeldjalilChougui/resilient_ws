import 'dart:async';
import 'dart:convert';
import 'dart:math' as math;

import 'package:http/http.dart' as http;

import '../connection_status.dart';
import '../exceptions.dart';
import '../token_auth.dart';
import 'sse_event.dart';
import 'sse_parser.dart';

/// Reported when the server answered with a status that stops the client.
class SseHttpStatusException implements Exception {
  /// Creates the exception for [statusCode].
  const SseHttpStatusException(this.statusCode);

  /// The HTTP status code.
  final int statusCode;

  @override
  String toString() => 'SseHttpStatusException($statusCode)';
}

/// Reported when a 200 response is not `text/event-stream`. The standard
/// says to fail the connection without reconnecting.
class SseContentTypeException implements Exception {
  /// Creates the exception for [contentType].
  const SseContentTypeException(this.contentType);

  /// The Content-Type the server sent, if any.
  final String? contentType;

  @override
  String toString() => 'SseContentTypeException($contentType)';
}

/// A Server-Sent Events client built on `package:http`, with the reconnect
/// behaviour of a browser `EventSource` and the extras a mobile app needs.
///
/// * Parses the stream with [SseParser] (WHATWG HTML, server-sent events).
/// * After a connection ends, waits the reconnection time (the server's
///   `retry:` field, or [retry]) and reconnects with `Last-Event-ID`.
///   Consecutive failed attempts double the wait, up to [maxRetry] (a server
///   `retry:` above [maxRetry] is still honoured).
/// * Calls the token provider before every request and sends
///   `Authorization: Bearer <token>` (configurable).
/// * HTTP 204 stops the client, as the standard says. Other statuses stop it
///   unless [shouldRetryStatus] says otherwise; by default 5xx, 408 and 429
///   are retried and every other 4xx stops.
/// * [idleTimeout] reconnects when nothing, not even a comment, arrives for
///   that long, which catches connections silently dropped by the network.
/// * An optional `networkAvailable` stream pauses reconnects while offline.
///
/// On the web, `http.Client()` is the fetch-based `BrowserClient`, which
/// streams response bodies (package:http 1.3 and later).
class SseClient {
  /// Creates a client for [url]. Nothing happens until [connect].
  ///
  /// If [client] is given it is used for every request and is not closed by
  /// [close]; otherwise the client creates and owns one.
  SseClient({
    required this.url,
    http.Client? client,
    TokenProvider? tokenProvider,
    this.authHeader = 'Authorization',
    this.tokenPrefix = 'Bearer ',
    Map<String, String> headers = const {},
    this.retry = const Duration(seconds: 3),
    this.maxRetry = const Duration(seconds: 30),
    bool Function(int statusCode)? shouldRetryStatus,
    String lastEventId = '',
    this.idleTimeout,
    this.connectTimeout = const Duration(seconds: 30),
    Stream<bool>? networkAvailable,
    this.maxAttempts,
  })  : _client = client ?? http.Client(),
        _ownsClient = client == null,
        _tokenProvider = tokenProvider,
        _headers = Map.unmodifiable(headers),
        _shouldRetryStatus = shouldRetryStatus ?? defaultShouldRetryStatus,
        _lastEventId = lastEventId,
        _reconnectionTime = retry {
    if (maxAttempts != null && maxAttempts! < 1) {
      throw ArgumentError.value(maxAttempts, 'maxAttempts', 'must be >= 1');
    }
    _networkSub = networkAvailable?.listen(_onNetwork);
  }

  /// Default status policy: retry 5xx, 408 and 429; stop on anything else.
  static bool defaultShouldRetryStatus(int statusCode) =>
      statusCode >= 500 || statusCode == 408 || statusCode == 429;

  /// The event stream URL.
  final Uri url;

  /// Header that carries the token.
  final String authHeader;

  /// Text placed before the token in [authHeader].
  final String tokenPrefix;

  /// Reconnection time until the server sends a `retry:` field.
  final Duration retry;

  /// Cap for the growing wait between consecutive failed attempts.
  final Duration maxRetry;

  /// Reconnect if no bytes arrive for this long. Null disables it.
  final Duration? idleTimeout;

  /// Maximum time to wait for response headers.
  final Duration connectTimeout;

  /// Consecutive failed attempts after which the client gives up, or null to
  /// retry forever.
  final int? maxAttempts;

  final http.Client _client;
  final bool _ownsClient;
  final TokenProvider? _tokenProvider;
  final Map<String, String> _headers;
  final bool Function(int) _shouldRetryStatus;

  final StreamController<SseEvent> _events =
      StreamController<SseEvent>.broadcast();
  final StreamController<ConnectionStatus> _states =
      StreamController<ConnectionStatus>.broadcast();
  final StreamController<Object> _errors = StreamController<Object>.broadcast();

  ConnectionStatus _state = ConnectionStatus.idle;
  String _lastEventId;
  Duration _reconnectionTime;
  StreamSubscription<String>? _sub;
  StreamSubscription<bool>? _networkSub;
  Completer<void>? _abort;
  Timer? _retryTimer;
  Timer? _idleTimer;
  int _gen = 0;
  int _failures = 0;
  bool _inFlight = false;
  bool _networkUp = true;
  int? _stopStatusCode;

  /// Dispatched events.
  Stream<SseEvent> get events => _events.stream;

  /// State changes. Broadcast; closes after [ConnectionStatus.closed].
  Stream<ConnectionStatus> get states => _states.stream;

  /// The current state.
  ConnectionStatus get state => _state;

  /// Connection problems: network errors, retried statuses,
  /// [SseHttpStatusException], [SseContentTypeException] and
  /// [RetryLimitExceededException].
  Stream<Object> get errors => _errors.stream;

  /// The last event ID; sent as `Last-Event-ID` on the next request.
  String get lastEventId => _lastEventId;

  /// The current reconnection time.
  Duration get reconnectionTime => _reconnectionTime;

  /// The HTTP status that stopped the client (204 or a non-retried
  /// status), if any.
  int? get stopStatusCode => _stopStatusCode;

  /// Starts the event stream. Calling it again while running does nothing.
  ///
  /// Throws [StateError] after [close].
  void connect() {
    if (_state == ConnectionStatus.closed) {
      throw StateError('SseClient is closed');
    }
    if (_state != ConnectionStatus.idle) return;
    _setState(ConnectionStatus.connecting);
    _startAttempt();
  }

  /// Aborts the request and stops for good. Streams close. Safe to call more
  /// than once.
  Future<void> close() async {
    if (_state == ConnectionStatus.closed) return;
    final sub = _sub;
    _sub = null;
    _terminate();
    await sub?.cancel();
  }

  // ---------------------------------------------------------------------

  void _setState(ConnectionStatus s) {
    if (_state == s) return;
    _state = s;
    if (!_states.isClosed) _states.add(s);
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
    final abort = Completer<void>();
    _abort = abort;
    final http.StreamedResponse response;
    try {
      final token = await _tokenProvider?.call();
      if (gen != _gen) return;
      final request =
          http.AbortableRequest('GET', url, abortTrigger: abort.future)
            ..headers.addAll({
              ..._headers,
              'Accept': 'text/event-stream',
              'Cache-Control': 'no-store',
              if (_lastEventId.isNotEmpty) 'Last-Event-ID': _lastEventId,
              if (token != null) authHeader: '$tokenPrefix$token',
            });
      response = await _client.send(request).timeout(connectTimeout);
    } on Object catch (e) {
      if (gen != _gen) return;
      _inFlight = false;
      _abortRequest(abort);
      _errors.add(e);
      _onAttemptFailed();
      return;
    }
    if (gen != _gen) {
      _abortRequest(abort);
      _drain(response);
      return;
    }
    _inFlight = false;

    final status = response.statusCode;
    if (status != 200) {
      _drain(response);
      if (status == 204) {
        _stopStatusCode = 204;
        _terminate();
      } else if (_shouldRetryStatus(status)) {
        _errors.add(SseHttpStatusException(status));
        _onAttemptFailed();
      } else {
        _stopStatusCode = status;
        _errors.add(SseHttpStatusException(status));
        _terminate();
      }
      return;
    }
    final contentType = response.headers['content-type'];
    final mime = contentType?.split(';').first.trim().toLowerCase();
    if (mime != 'text/event-stream') {
      _drain(response);
      _errors.add(SseContentTypeException(contentType));
      _terminate();
      return;
    }

    _failures = 0;
    _setState(ConnectionStatus.open);
    final parser = SseParser(lastEventId: _lastEventId)
      ..onRetry = (d) => _reconnectionTime = d;
    _resetIdle(gen);
    _sub = response.stream
        .transform(const Utf8Decoder(allowMalformed: true))
        .listen(
      (chunk) {
        if (gen != _gen) return;
        _resetIdle(gen);
        final dispatched = parser.add(chunk);
        _lastEventId = parser.lastEventId;
        for (final e in dispatched) {
          _events.add(e);
        }
      },
      onError: (Object e) {
        if (gen == _gen && e is! http.RequestAbortedException) {
          _errors.add(e);
        }
      },
      onDone: () {
        if (gen != _gen) return;
        parser.close();
        _onDropped();
      },
      cancelOnError: true,
    );
  }

  void _resetIdle(int gen) {
    final timeout = idleTimeout;
    if (timeout == null) return;
    _idleTimer?.cancel();
    _idleTimer = Timer(timeout, () {
      if (gen != _gen) return;
      _gen++;
      final sub = _sub;
      _sub = null;
      unawaited(sub?.cancel());
      _abortRequest(_abort);
      _errors.add(TimeoutException('No data from the event stream', timeout));
      _onDropped();
    });
  }

  void _onDropped() {
    _idleTimer?.cancel();
    _sub = null;
    _setState(ConnectionStatus.reconnecting);
    _schedule(_reconnectionTime);
  }

  void _onAttemptFailed() {
    _failures++;
    final limit = maxAttempts;
    if (limit != null && _failures >= limit) {
      _errors.add(RetryLimitExceededException(_failures));
      _terminate();
      return;
    }
    _setState(ConnectionStatus.reconnecting);
    // retry, 2 * retry, 4 * retry ... capped at maxRetry (but never below the
    // server's own reconnection time).
    final cap =
        math.max(maxRetry.inMicroseconds, _reconnectionTime.inMicroseconds);
    var delay = _reconnectionTime.inMicroseconds.toDouble();
    for (var i = 1; i < _failures && delay < cap; i++) {
      delay *= 2;
    }
    _schedule(Duration(microseconds: math.min(delay, cap.toDouble()).round()));
  }

  void _schedule(Duration delay) {
    if (!_networkUp) return;
    _retryTimer?.cancel();
    _retryTimer = Timer(delay, _startAttempt);
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
    if (waiting && !_inFlight && _sub == null) {
      _failures = 0;
      _startAttempt();
    }
  }

  void _abortRequest(Completer<void>? abort) {
    if (abort != null && !abort.isCompleted) abort.complete();
  }

  void _drain(http.StreamedResponse response) {
    response.stream.listen(null, onError: (Object _) {}).cancel().ignore();
  }

  void _terminate() {
    _gen++;
    _retryTimer?.cancel();
    _idleTimer?.cancel();
    _abortRequest(_abort);
    unawaited(_networkSub?.cancel());
    _networkSub = null;
    _setState(ConnectionStatus.closed);
    if (_ownsClient) _client.close();
    unawaited(_events.close());
    unawaited(_states.close());
    unawaited(_errors.close());
  }
}
