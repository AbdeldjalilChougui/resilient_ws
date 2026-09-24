@TestOn('vm')
library;

import 'dart:async';
import 'dart:io';

import 'package:http/http.dart' as http;
import 'package:resilient_ws/resilient_ws.dart';
import 'package:test/test.dart';

import 'support/ws_test_server.dart' show eventually;

/// A local SSE endpoint whose per-request behaviour the test scripts.
class SseServer {
  SseServer._(this._server) {
    _server.listen((req) {
      requests.add(req);
      final i = requests.length - 1;
      final handler = i < handlers.length ? handlers[i] : fallback;
      handler(req);
    });
  }

  static Future<SseServer> start() async =>
      SseServer._(await HttpServer.bind(InternetAddress.loopbackIPv4, 0));

  final HttpServer _server;
  final List<HttpRequest> requests = [];

  /// Handler for request i; requests beyond the list use [fallback].
  final List<void Function(HttpRequest)> handlers = [];
  void Function(HttpRequest) fallback = hold;

  Uri get url => Uri.parse('http://127.0.0.1:${_server.port}/events');

  Future<void> close() => _server.close(force: true);

  /// Starts an event stream and writes [body], then keeps it open.
  static void Function(HttpRequest) stream(String body, {bool end = false}) =>
      (req) async {
        req.response
          ..statusCode = 200
          ..headers.contentType =
              ContentType('text', 'event-stream', charset: 'utf-8')
          ..bufferOutput = false;
        req.response.write(body);
        await req.response.flush();
        if (end) await req.response.close();
      };

  static void hold(HttpRequest req) => stream(': open\n\n')(req);

  static void Function(HttpRequest) status(int code,
          {String contentType = 'text/plain'}) =>
      (req) async {
        req.response
          ..statusCode = code
          ..headers.set('content-type', contentType);
        await req.response.close();
      };
}

void main() {
  late SseServer server;
  final clients = <SseClient>[];

  setUp(() async => server = await SseServer.start());
  tearDown(() async {
    for (final c in clients) {
      await c.close();
    }
    clients.clear();
    await server.close();
  });

  SseClient make({
    TokenProvider? tokenProvider,
    String lastEventId = '',
    bool Function(int status)? shouldRetryStatus,
    http.Client? client,
    Duration retry = const Duration(milliseconds: 20),
  }) {
    final c = SseClient(
      url: server.url,
      client: client,
      tokenProvider: tokenProvider,
      lastEventId: lastEventId,
      shouldRetryStatus: shouldRetryStatus,
      retry: retry,
      maxRetry: const Duration(milliseconds: 100),
    );
    clients.add(c);
    return c;
  }

  test('receives events and sends SSE request headers', () async {
    server.handlers.add(SseServer.stream(
        'event: greet\ndata: hello\ndata: world\n\n: heartbeat\n\n'));
    final c = make(tokenProvider: () => 'abc');
    final states = <ConnectionStatus>[];
    c.states.listen(states.add);
    final events = <SseEvent>[];
    c.events.listen(events.add);
    c.connect();
    await eventually(() => events.isNotEmpty);
    expect(events.single.type, 'greet');
    expect(events.single.data, 'hello\nworld');
    expect(states, [ConnectionStatus.connecting, ConnectionStatus.open]);
    final req = server.requests.single;
    expect(req.headers.value('accept'), 'text/event-stream');
    expect(req.headers.value('authorization'), 'Bearer abc');
    expect(req.headers.value('last-event-id'), isNull);
  });

  test('decodes UTF-8 split across network writes and skips the BOM', () async {
    server.handlers.add((req) async {
      req.response
        ..statusCode = 200
        ..headers.set('content-type', 'text/event-stream')
        ..bufferOutput = false;
      // BOM + "data: é\n\n", with é (C3 A9) split between two writes.
      req.response.add([0xEF, 0xBB, 0xBF, ...'data: '.codeUnits, 0xC3]);
      await req.response.flush();
      await Future<void>.delayed(const Duration(milliseconds: 20));
      req.response.add([0xA9, 0x0A, 0x0A]);
      await req.response.flush();
    });
    final c = make();
    final events = <SseEvent>[];
    c.events.listen(events.add);
    c.connect();
    await eventually(() => events.isNotEmpty);
    expect(events.single.data, 'é');
  });

  test('reconnects with Last-Event-ID and a fresh token', () async {
    server.handlers
      ..add(SseServer.stream(
          'retry: 30\nid: 1\ndata: a\n\nid: 2\ndata: b\n\nid: 9\ndata: cut',
          end: true))
      ..add(SseServer.stream('data: c\n\n'));
    var n = 0;
    final c = make(tokenProvider: () => 't${++n}');
    final events = <SseEvent>[];
    c.events.listen(events.add);
    c.connect();
    await eventually(() => events.length == 3);
    expect(events.map((e) => e.data), ['a', 'b', 'c']);
    expect(events.last.lastEventId, '2');
    final second = server.requests[1];
    // "id: 9" never reached a blank line, so it was not committed.
    expect(second.headers.value('last-event-id'), '2');
    expect(server.requests[0].headers.value('authorization'), 'Bearer t1');
    expect(second.headers.value('authorization'), 'Bearer t2');
    expect(c.lastEventId, '2');
  });

  test('an initial lastEventId is sent on the first request', () async {
    final c = make(lastEventId: '77');
    c.connect();
    await eventually(() => server.requests.isNotEmpty);
    expect(server.requests.single.headers.value('last-event-id'), '77');
  });

  test('HTTP 204 stops reconnecting', () async {
    server.handlers.add(SseServer.status(204));
    final c = make();
    c.connect();
    await eventually(() => c.state == ConnectionStatus.closed);
    await Future<void>.delayed(const Duration(milliseconds: 150));
    expect(server.requests, hasLength(1));
    expect(c.stopStatusCode, 204);
  });

  test('4xx stops reconnecting by default', () async {
    server.handlers.add(SseServer.status(401));
    final errors = <Object>[];
    final c = make();
    c.errors.listen(errors.add);
    c.connect();
    await eventually(() => c.state == ConnectionStatus.closed);
    await Future<void>.delayed(const Duration(milliseconds: 150));
    expect(server.requests, hasLength(1));
    expect(errors.whereType<SseHttpStatusException>().single.statusCode, 401);
    expect(c.stopStatusCode, 401);
  });

  test('4xx handling is configurable', () async {
    server.handlers
      ..add(SseServer.status(401))
      ..add(SseServer.stream('data: ok\n\n'));
    final c = make(shouldRetryStatus: (s) => s == 401);
    final events = <SseEvent>[];
    c.events.listen(events.add);
    c.connect();
    await eventually(() => events.isNotEmpty);
    expect(server.requests, hasLength(2));
  });

  test('5xx is retried', () async {
    server.handlers
      ..add(SseServer.status(503))
      ..add(SseServer.status(500))
      ..add(SseServer.stream('data: back\n\n'));
    final c = make();
    final events = <SseEvent>[];
    c.events.listen(events.add);
    c.connect();
    await eventually(() => events.isNotEmpty);
    expect(server.requests, hasLength(3));
    expect(c.state, ConnectionStatus.open);
  });

  test('a 200 that is not text/event-stream fails permanently', () async {
    server.handlers.add(SseServer.status(200, contentType: 'text/html'));
    final errors = <Object>[];
    final c = make();
    c.errors.listen(errors.add);
    c.connect();
    await eventually(() => c.state == ConnectionStatus.closed);
    expect(errors.single, isA<SseContentTypeException>());
    await Future<void>.delayed(const Duration(milliseconds: 100));
    expect(server.requests, hasLength(1));
  });

  test('close() ends the stream and does not reconnect', () async {
    final c = make();
    final done = c.events.toList();
    c.connect();
    await eventually(() => c.state == ConnectionStatus.open);
    await c.close();
    expect(await done.timeout(const Duration(seconds: 2)), isEmpty);
    await Future<void>.delayed(const Duration(milliseconds: 150));
    expect(server.requests, hasLength(1));
    expect(c.state, ConnectionStatus.closed);
    expect(c.connect, throwsStateError);
  });

  test('an injected client is used and left open', () async {
    final inner = http.Client();
    final counting = _CountingClient(inner);
    final c = make(client: counting);
    c.connect();
    await eventually(() => c.state == ConnectionStatus.open);
    await c.close();
    expect(counting.sent, 1);
    expect(counting.closed, isFalse);
    inner.close();
  });
}

class _CountingClient extends http.BaseClient {
  _CountingClient(this._inner);
  final http.Client _inner;
  int sent = 0;
  bool closed = false;

  @override
  Future<http.StreamedResponse> send(http.BaseRequest request) {
    sent++;
    return _inner.send(request);
  }

  @override
  void close() => closed = true;
}
