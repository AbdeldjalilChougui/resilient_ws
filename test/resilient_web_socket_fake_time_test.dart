import 'dart:async';
import 'dart:convert';

import 'package:fake_async/fake_async.dart';
import 'package:resilient_ws/resilient_ws.dart';
import 'package:test/test.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

import 'support/fake_channel.dart';

/// Records every channel the socket asks for, with the fake time it asked.
class Harness {
  Harness(this.async);
  final FakeAsync async;
  final List<FakeChannel> channels = [];
  final List<Duration> connectTimes = [];

  /// When true, every new channel accepts its handshake immediately.
  bool autoAccept = true;

  WebSocketChannel connect(Uri url,
      {required Map<String, String> headers, Iterable<String>? protocols}) {
    final c = FakeChannel(url, headers);
    channels.add(c);
    connectTimes.add(async.elapsed);
    if (autoAccept) c.accept();
    return c;
  }
}

ResilientWebSocket<Object?> socket(
  Harness h, {
  Backoff? backoff,
  Heartbeat? heartbeat,
  Duration stableAfter = const Duration(seconds: 10),
  Stream<bool>? network,
  TokenProvider? tokenProvider,
  Duration connectTimeout = const Duration(seconds: 10),
}) =>
    ResilientWebSocket<Object?>(
      url: Uri.parse('ws://example.test/ws'),
      codec: const JsonMessageCodec<Object?>(),
      connector: h.connect,
      backoff: backoff ??
          Backoff(
            initial: const Duration(seconds: 1),
            max: const Duration(seconds: 30),
            jitter: 0,
          ),
      heartbeat: heartbeat,
      stableAfter: stableAfter,
      networkAvailable: network,
      tokenProvider: tokenProvider,
      connectTimeout: connectTimeout,
    );

void main() {
  test('backoff delays are 1, 2, 4, 8, 16, 30, 30 seconds', () {
    fakeAsync((async) {
      final h = Harness(async)..autoAccept = false;
      final ws = socket(h);
      ws.connect();
      async.flushMicrotasks();
      final delays = <Duration>[];
      for (var i = 0; i < 7; i++) {
        final count = h.channels.length;
        final rejectedAt = async.elapsed;
        h.channels.last.reject();
        // Advance one second at a time until the next attempt appears.
        while (h.channels.length == count) {
          async.elapse(const Duration(milliseconds: 100));
        }
        delays.add(h.connectTimes.last - rejectedAt);
      }
      expect(delays.map((d) => d.inMilliseconds),
          [1000, 2000, 4000, 8000, 16000, 30000, 30000]);
      ws.close();
      async.flushMicrotasks();
    });
  });

  test('a handshake that never completes times out and is retried', () {
    fakeAsync((async) {
      final h = Harness(async)..autoAccept = false;
      final errors = <Object>[];
      final ws = socket(h, connectTimeout: const Duration(seconds: 5));
      ws.errors.listen(errors.add);
      ws.connect();
      async.elapse(const Duration(seconds: 5, milliseconds: 1));
      expect(errors.single, isA<TimeoutException>());
      expect(h.channels.single.clientClosed, isTrue);
      async.elapse(const Duration(seconds: 1));
      expect(h.channels, hasLength(2));
      ws.close();
      async.flushMicrotasks();
    });
  });

  test('backoff resets once a connection stayed up for stableAfter', () {
    fakeAsync((async) {
      final h = Harness(async);
      final ws = socket(h, stableAfter: const Duration(seconds: 10));
      ws.connect();
      async.flushMicrotasks();

      // Flapping: each connection dies right away, so delays keep growing.
      final delays = <Duration>[];
      for (var i = 0; i < 3; i++) {
        final count = h.channels.length;
        final droppedAt = async.elapsed;
        h.channels.last.serverClose(1011);
        while (h.channels.length == count) {
          async.elapse(const Duration(milliseconds: 100));
        }
        delays.add(h.connectTimes.last - droppedAt);
      }
      expect(delays.map((d) => d.inSeconds), [1, 2, 4]);

      // Now the connection survives 10 s: the next drop starts at 1 s again.
      async.elapse(const Duration(seconds: 10));
      final count = h.channels.length;
      final droppedAt = async.elapsed;
      h.channels.last.serverClose(1011);
      while (h.channels.length == count) {
        async.elapse(const Duration(milliseconds: 100));
      }
      expect((h.connectTimes.last - droppedAt).inSeconds, 1);
      ws.close();
      async.flushMicrotasks();
    });
  });

  test('heartbeat: pings on interval, reconnects when a pong is late', () {
    fakeAsync((async) {
      final h = Harness(async);
      final errors = <Object>[];
      final ws = socket(
        h,
        heartbeat: const Heartbeat(
          interval: Duration(seconds: 20),
          timeout: Duration(seconds: 5),
        ),
      );
      ws.errors.listen(errors.add);
      ws.connect();
      async.flushMicrotasks();
      final first = h.channels.single;

      async.elapse(const Duration(seconds: 20));
      expect(first.sent, [
        jsonEncode({'type': 'ping'})
      ]);
      first.push(jsonEncode({'type': 'pong'}));
      async.elapse(const Duration(seconds: 10));
      expect(h.channels, hasLength(1));

      // Second ping at t=40 s gets no answer; at t=45 s the socket is dropped.
      async.elapse(const Duration(seconds: 10));
      expect(first.sent, hasLength(2));
      async.elapse(const Duration(seconds: 4));
      expect(first.clientClosed, isFalse);
      async.elapse(const Duration(seconds: 1));
      expect(first.clientClosed, isTrue);
      expect(errors.single, isA<HeartbeatTimeoutException>());
      expect(ws.state, ConnectionStatus.reconnecting);
      async.elapse(const Duration(seconds: 1));
      expect(h.channels, hasLength(2));
      expect(ws.state, ConnectionStatus.open);
      ws.close();
      async.flushMicrotasks();
    });
  });

  test('heartbeat: any inbound frame proves the link is alive', () {
    fakeAsync((async) {
      final h = Harness(async);
      final ws = socket(
        h,
        heartbeat: const Heartbeat(
          interval: Duration(seconds: 20),
          timeout: Duration(seconds: 5),
        ),
      );
      ws.connect();
      async.flushMicrotasks();
      async.elapse(const Duration(seconds: 21));
      h.channels.single.push(jsonEncode({'data': 1}));
      async.elapse(const Duration(seconds: 10));
      expect(h.channels, hasLength(1));
      ws.close();
      async.flushMicrotasks();
    });
  });

  test('network loss pauses reconnects; its return reconnects at once', () {
    fakeAsync((async) {
      final h = Harness(async);
      final network = StreamController<bool>(sync: true);
      final ws = socket(h, network: network.stream);
      ws.connect();
      async.flushMicrotasks();

      // Build up some backoff first.
      for (var i = 0; i < 3; i++) {
        final count = h.channels.length;
        h.channels.last.serverClose(1011);
        while (h.channels.length == count) {
          async.elapse(const Duration(milliseconds: 100));
        }
      }
      network.add(false);
      h.channels.last.serverClose(1006);
      async.elapse(const Duration(minutes: 5));
      expect(h.channels, hasLength(4));
      expect(ws.state, ConnectionStatus.reconnecting);

      network.add(true);
      async.flushMicrotasks();
      expect(h.channels, hasLength(5));
      expect(ws.state, ConnectionStatus.open);
      ws.close();
      network.close();
      async.flushMicrotasks();
    });
  });

  test('connect() while the network is down waits for it', () {
    fakeAsync((async) {
      final h = Harness(async);
      final network = StreamController<bool>(sync: true);
      final ws = socket(h, network: network.stream);
      network.add(false);
      ws.connect();
      async.elapse(const Duration(minutes: 1));
      expect(h.channels, isEmpty);
      network.add(true);
      async.flushMicrotasks();
      expect(h.channels, hasLength(1));
      ws.close();
      network.close();
      async.flushMicrotasks();
    });
  });

  test('network going away while open leaves the connection alone', () {
    fakeAsync((async) {
      final h = Harness(async);
      final network = StreamController<bool>(sync: true);
      final ws = socket(h, network: network.stream);
      ws.connect();
      async.flushMicrotasks();
      network.add(false);
      network.add(true);
      async.elapse(const Duration(seconds: 5));
      expect(h.channels, hasLength(1));
      expect(ws.state, ConnectionStatus.open);
      ws.close();
      network.close();
      async.flushMicrotasks();
    });
  });

  test('frames from a replaced connection are ignored', () {
    fakeAsync((async) {
      final h = Harness(async);
      final ws = socket(h);
      final received = <Object?>[];
      ws.messages.listen(received.add);
      ws.connect();
      async.flushMicrotasks();
      final first = h.channels.single;
      ws.reconnect();
      async.flushMicrotasks();
      expect(h.channels, hasLength(2));
      expect(first.clientClosed, isTrue);
      h.channels.last.push(jsonEncode('new'));
      async.flushMicrotasks();
      expect(received, ['new']);
      ws.close();
      async.flushMicrotasks();
    });
  });

  test('token provider is called on every connect', () {
    fakeAsync((async) {
      final h = Harness(async);
      var n = 0;
      final ws = socket(h, tokenProvider: () => 'tok${++n}');
      ws.connect();
      async.flushMicrotasks();
      h.channels.last.serverClose(1011);
      async.elapse(const Duration(seconds: 1));
      h.channels.last.serverClose(1011);
      async.elapse(const Duration(seconds: 2));
      expect(h.channels.map((c) => c.url.queryParameters['token']),
          ['tok1', 'tok2', 'tok3']);
      ws.close();
      async.flushMicrotasks();
    });
  });
}
