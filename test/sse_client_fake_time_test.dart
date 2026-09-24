import 'dart:async';
import 'dart:convert';

import 'package:fake_async/fake_async.dart';
import 'package:http/http.dart' as http;
import 'package:http/testing.dart';
import 'package:resilient_ws/resilient_ws.dart';
import 'package:test/test.dart';

/// A scripted server: each request gets the next response builder.
class Script {
  Script(this.async);
  final FakeAsync async;
  final List<Duration> requestTimes = [];
  final List<http.BaseRequest> requests = [];
  final List<StreamController<List<int>>> bodies = [];

  /// Status per request; defaults to 200 once the list runs out.
  final List<int> statuses = [];

  late final http.Client client = MockClient.streaming((req, _) async {
    requests.add(req);
    requestTimes.add(async.elapsed);
    final i = requests.length - 1;
    final status = i < statuses.length ? statuses[i] : 200;
    final body = StreamController<List<int>>();
    bodies.add(body);
    if (status != 200) unawaited(body.close());
    return http.StreamedResponse(body.stream, status,
        headers: {'content-type': 'text/event-stream'});
  });

  void write(String text) => bodies.last.add(utf8.encode(text));
  void end() => bodies.last.close();
}

SseClient make(
  Script s, {
  Duration retry = const Duration(seconds: 3),
  Duration maxRetry = const Duration(seconds: 30),
  Duration? idleTimeout,
  Stream<bool>? network,
}) =>
    SseClient(
      url: Uri.parse('https://example.test/events'),
      client: s.client,
      retry: retry,
      maxRetry: maxRetry,
      idleTimeout: idleTimeout,
      networkAvailable: network,
    );

void main() {
  test('honours the server retry field exactly', () {
    fakeAsync((async) {
      final s = Script(async);
      final c = make(s);
      c.connect();
      async.flushMicrotasks();
      s.write('retry: 5000\ndata: x\n\n');
      async.flushMicrotasks();
      expect(c.reconnectionTime, const Duration(seconds: 5));
      final droppedAt = async.elapsed;
      s.end();
      async.elapse(const Duration(milliseconds: 4999));
      expect(s.requests, hasLength(1));
      expect(c.state, ConnectionStatus.reconnecting);
      async.elapse(const Duration(milliseconds: 1));
      expect(s.requests, hasLength(2));
      expect(s.requestTimes.last - droppedAt, const Duration(seconds: 5));
      c.close();
      async.flushMicrotasks();
    });
  });

  test('without a retry field the configured default is used', () {
    fakeAsync((async) {
      final s = Script(async);
      final c = make(s, retry: const Duration(seconds: 3));
      c.connect();
      async.flushMicrotasks();
      s.end();
      async.elapse(const Duration(seconds: 3));
      expect(s.requests, hasLength(2));
      c.close();
      async.flushMicrotasks();
    });
  });

  test('consecutive failures back off from the retry time up to maxRetry', () {
    fakeAsync((async) {
      final s = Script(async)..statuses.addAll(List.filled(6, 503));
      final c = make(s,
          retry: const Duration(seconds: 1),
          maxRetry: const Duration(seconds: 10));
      c.connect();
      async.elapse(const Duration(minutes: 1));
      final gaps = [
        for (var i = 1; i < 7; i++)
          (s.requestTimes[i] - s.requestTimes[i - 1]).inSeconds,
      ];
      expect(gaps, [1, 2, 4, 8, 10, 10]);
      // The 7th request succeeded, so the counter is back to zero.
      expect(c.state, ConnectionStatus.open);
      c.close();
      async.flushMicrotasks();
    });
  });

  test('a server retry above maxRetry is still honoured', () {
    fakeAsync((async) {
      final s = Script(async);
      final c = make(s, maxRetry: const Duration(seconds: 10));
      c.connect();
      async.flushMicrotasks();
      s.write('retry: 60000\n\n');
      s.end();
      async.elapse(const Duration(seconds: 59));
      expect(s.requests, hasLength(1));
      async.elapse(const Duration(seconds: 1));
      expect(s.requests, hasLength(2));
      c.close();
      async.flushMicrotasks();
    });
  });

  test('idleTimeout reconnects when nothing, not even a comment, arrives', () {
    fakeAsync((async) {
      final s = Script(async);
      final c = make(s,
          retry: const Duration(seconds: 1),
          idleTimeout: const Duration(seconds: 30));
      c.connect();
      async.flushMicrotasks();
      async.elapse(const Duration(seconds: 20));
      s.write(': keep-alive\n\n');
      async.elapse(const Duration(seconds: 20));
      expect(s.requests, hasLength(1));
      async.elapse(const Duration(seconds: 10));
      expect(c.state, ConnectionStatus.reconnecting);
      async.elapse(const Duration(seconds: 1));
      expect(s.requests, hasLength(2));
      c.close();
      async.flushMicrotasks();
    });
  });

  test('network loss pauses reconnects; its return reconnects at once', () {
    fakeAsync((async) {
      final s = Script(async);
      final network = StreamController<bool>(sync: true);
      final c = make(s, network: network.stream);
      c.connect();
      async.flushMicrotasks();
      network.add(false);
      s.end();
      async.elapse(const Duration(minutes: 10));
      expect(s.requests, hasLength(1));
      network.add(true);
      async.flushMicrotasks();
      expect(s.requests, hasLength(2));
      c.close();
      network.close();
      async.flushMicrotasks();
    });
  });

  test('no Last-Event-ID header when the ID is empty', () {
    fakeAsync((async) {
      final s = Script(async);
      final c = make(s);
      c.connect();
      async.flushMicrotasks();
      s.write('id: 3\ndata: a\n\nid\ndata: b\n\n');
      s.end();
      async.elapse(const Duration(seconds: 3));
      expect(s.requests, hasLength(2));
      expect(s.requests[1].headers.containsKey('Last-Event-ID'), isFalse);
      c.close();
      async.flushMicrotasks();
    });
  });

  test('maxAttempts stops after consecutive failures', () {
    fakeAsync((async) {
      final s = Script(async)..statuses.addAll(List.filled(10, 502));
      final errors = <Object>[];
      final c = SseClient(
        url: Uri.parse('https://example.test/events'),
        client: s.client,
        retry: const Duration(seconds: 1),
        maxAttempts: 3,
      );
      c.errors.listen(errors.add);
      c.connect();
      async.elapse(const Duration(minutes: 1));
      expect(s.requests, hasLength(3));
      expect(c.state, ConnectionStatus.closed);
      expect(errors.last, isA<RetryLimitExceededException>());
    });
  });

  test('custom headers and token header are sent', () {
    fakeAsync((async) {
      final s = Script(async);
      final c = SseClient(
        url: Uri.parse('https://example.test/events'),
        client: s.client,
        headers: {'X-App': 'demo'},
        tokenProvider: () async => 'k1',
        authHeader: 'X-Api-Key',
        tokenPrefix: '',
      );
      c.connect();
      async.flushMicrotasks();
      final h = s.requests.single.headers;
      expect(h['X-App'], 'demo');
      expect(h['X-Api-Key'], 'k1');
      expect(h['Cache-Control'], 'no-store');
      c.close();
      async.flushMicrotasks();
    });
  });
}
