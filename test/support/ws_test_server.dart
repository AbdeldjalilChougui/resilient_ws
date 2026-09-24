import 'dart:async';
import 'dart:convert';
import 'dart:io';

/// One accepted WebSocket connection on [TestWsServer].
class ServerConn {
  ServerConn(this.uri, this.headers, this.socket, {required this.server}) {
    socket.listen(
      (data) {
        received.add(data);
        if (server.respondToPings && data is String) {
          try {
            final m = jsonDecode(data);
            if (m is Map && m['type'] == 'ping') {
              socket.add(jsonEncode({'type': 'pong'}));
              return;
            }
          } on FormatException {
            // not JSON
          }
        }
      },
      onDone: () => _done.complete(),
    );
  }

  final Uri uri;
  final HttpHeaders headers;
  final WebSocket socket;
  final TestWsServer server;
  final List<Object?> received = [];
  final Completer<void> _done = Completer<void>();

  /// Completes when the client side closed or the socket died.
  Future<void> get done => _done.future;

  /// Non-ping frames received from the client, decoded as JSON.
  List<Object?> get messages => [
        for (final r in received)
          if (r is String) jsonDecode(r),
      ].where((m) => !(m is Map && m['type'] == 'ping')).toList();

  void sendJson(Object? value) => socket.add(jsonEncode(value));

  Future<void> close([int? code, String? reason]) => socket.close(code, reason);
}

/// A real in-process WebSocket server built on `dart:io`.
class TestWsServer {
  TestWsServer._(this._server) {
    _server.listen((req) async {
      if (!WebSocketTransformer.isUpgradeRequest(req)) {
        req.response.statusCode = HttpStatus.badRequest;
        await req.response.close();
        return;
      }
      final ws = await WebSocketTransformer.upgrade(req);
      final conn = ServerConn(req.uri, req.headers, ws, server: this);
      connections.add(conn);
      _connections.add(conn);
    });
  }

  static Future<TestWsServer> start() async =>
      TestWsServer._(await HttpServer.bind(InternetAddress.loopbackIPv4, 0));

  final HttpServer _server;
  final List<ServerConn> connections = [];
  final StreamController<ServerConn> _connections =
      StreamController<ServerConn>.broadcast();

  /// Whether `{"type":"ping"}` frames are answered with `{"type":"pong"}`.
  bool respondToPings = true;

  Uri get url => Uri.parse('ws://127.0.0.1:${_server.port}/ws');

  /// Waits until at least [count] connections have been accepted.
  Future<ServerConn> connection(int count,
      {Duration timeout = const Duration(seconds: 5)}) async {
    await eventually(() => connections.length >= count, timeout: timeout);
    return connections[count - 1];
  }

  Future<void> close() async {
    for (final c in connections) {
      await c.socket.close();
    }
    await _server.close(force: true);
  }
}

/// Polls [condition] until true, or fails after [timeout].
Future<void> eventually(bool Function() condition,
    {Duration timeout = const Duration(seconds: 5)}) async {
  final sw = Stopwatch()..start();
  while (!condition()) {
    if (sw.elapsed > timeout) {
      throw TimeoutException('condition not met within $timeout');
    }
    await Future<void>.delayed(const Duration(milliseconds: 5));
  }
}
