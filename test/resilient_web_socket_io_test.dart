@TestOn('vm')
library;

import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:resilient_ws/resilient_ws.dart';
import 'package:test/test.dart';

import 'support/ws_test_server.dart';

/// A typed message used to exercise the codec.
class Chat {
  Chat(this.text);
  factory Chat.fromJson(Object? json) =>
      Chat((json! as Map<String, Object?>)['text']! as String);
  final String text;
  Map<String, Object?> toJson() => {'text': text};
}

final chatCodec = JsonMessageCodec<Chat>(
  fromJson: Chat.fromJson,
  toJson: (c) => c.toJson(),
);

Backoff fastBackoff() => Backoff(
      initial: const Duration(milliseconds: 20),
      max: const Duration(milliseconds: 80),
      jitter: 0,
    );

void main() {
  late TestWsServer server;
  final sockets = <ResilientWebSocket<Object?>>[];

  setUp(() async {
    server = await TestWsServer.start();
  });

  tearDown(() async {
    for (final s in sockets) {
      await s.close();
    }
    sockets.clear();
    await server.close();
  });

  ResilientWebSocket<T> make<T>({
    required MessageCodec<T> codec,
    TokenProvider? tokenProvider,
    TokenAuth tokenAuth = const TokenAuth.queryParameter(),
    Heartbeat? heartbeat,
    int outboxCapacity = 100,
    OverflowPolicy overflowPolicy = OverflowPolicy.dropOldest,
    Set<int> permanentCloseCodes =
        ResilientWebSocket.defaultPermanentCloseCodes,
    int? maxAttempts,
    Uri? url,
  }) {
    final s = ResilientWebSocket<T>(
      url: url ?? server.url,
      codec: codec,
      tokenProvider: tokenProvider,
      tokenAuth: tokenAuth,
      backoff: fastBackoff(),
      heartbeat: heartbeat,
      outboxCapacity: outboxCapacity,
      overflowPolicy: overflowPolicy,
      permanentCloseCodes: permanentCloseCodes,
      maxAttempts: maxAttempts,
    );
    sockets.add(s as ResilientWebSocket<Object?>);
    return s;
  }

  test('connects, reports states and exchanges typed messages', () async {
    final ws = make(codec: chatCodec);
    final states = <ConnectionStatus>[];
    ws.states.listen(states.add);
    final received = <Chat>[];
    ws.messages.listen(received.add);

    expect(ws.state, ConnectionStatus.idle);
    ws.connect();
    final conn = await server.connection(1);
    await eventually(() => ws.state == ConnectionStatus.open);
    expect(states, [ConnectionStatus.connecting, ConnectionStatus.open]);

    ws.send(Chat('hello'));
    await eventually(() => conn.messages.isNotEmpty);
    expect(conn.messages.single, {'text': 'hello'});

    conn.sendJson({'text': 'hi back'});
    await eventually(() => received.isNotEmpty);
    expect(received.single.text, 'hi back');
  });

  test('undecodable frames go to errors, not messages', () async {
    final ws = make(codec: chatCodec);
    final errors = <Object>[];
    ws.errors.listen(errors.add);
    ws.connect();
    final conn = await server.connection(1);
    conn.socket.add('not json');
    await eventually(() => errors.isNotEmpty);
    expect(errors.single, isA<FormatException>());
    expect(ws.state, ConnectionStatus.open);
  });

  test('reconnects after the server drops the connection', () async {
    final ws = make(codec: const JsonMessageCodec<Object?>());
    final states = <ConnectionStatus>[];
    ws.states.listen(states.add);
    ws.connect();
    final first = await server.connection(1);
    await eventually(() => ws.state == ConnectionStatus.open);

    await first.close(1011, 'server restart');
    await server.connection(2);
    await eventually(() => ws.state == ConnectionStatus.open);
    expect(states, [
      ConnectionStatus.connecting,
      ConnectionStatus.open,
      ConnectionStatus.reconnecting,
      ConnectionStatus.open,
    ]);
  });

  test('re-sends topic subscriptions after every reconnect', () async {
    final ws = make(codec: const JsonMessageCodec<Object?>());
    ws
      ..subscribe('orders')
      ..subscribe('drivers');
    ws.connect();
    final first = await server.connection(1);
    await eventually(() => first.messages.length >= 2);
    expect(first.messages, [
      {'op': 'sub', 'topic': 'orders'},
      {'op': 'sub', 'topic': 'drivers'},
    ]);

    ws.unsubscribe('drivers');
    ws.subscribe('chat');
    await eventually(() => first.messages.length >= 4);
    expect(first.messages.sublist(2), [
      {'op': 'unsub', 'topic': 'drivers'},
      {'op': 'sub', 'topic': 'chat'},
    ]);
    expect(ws.topics, {'orders', 'chat'});

    await first.close(1001);
    final second = await server.connection(2);
    await eventually(() => second.messages.length >= 2);
    expect(second.messages, [
      {'op': 'sub', 'topic': 'orders'},
      {'op': 'sub', 'topic': 'chat'},
    ]);
  });

  test('subscribing twice sends one subscription', () async {
    final ws = make(codec: const JsonMessageCodec<Object?>());
    ws.connect();
    final conn = await server.connection(1);
    await eventually(() => ws.state == ConnectionStatus.open);
    ws
      ..subscribe('a')
      ..subscribe('a')
      ..unsubscribe('missing');
    await Future<void>.delayed(const Duration(milliseconds: 50));
    expect(conn.messages, [
      {'op': 'sub', 'topic': 'a'},
    ]);
  });

  test('custom subscription codec', () async {
    final ws = ResilientWebSocket<Object?>(
      url: server.url,
      codec: const JsonMessageCodec<Object?>(),
      backoff: fastBackoff(),
      subscriptionCodec: SubscriptionCodec.from(
        subscribe: (t) => 'SUB $t',
        unsubscribe: (t) => 'UNSUB $t',
      ),
    );
    sockets.add(ws);
    ws.subscribe('x');
    ws.connect();
    final conn = await server.connection(1);
    await eventually(() => conn.received.isNotEmpty);
    ws.unsubscribe('x');
    await eventually(() => conn.received.length >= 2);
    expect(conn.received, ['SUB x', 'UNSUB x']);
  });

  group('fresh token on every connect', () {
    Future<void> rotate(
        TokenAuth auth, String Function(ServerConn c) read) async {
      var n = 0;
      final ws = make(
        codec: const JsonMessageCodec<Object?>(),
        tokenAuth: auth,
        tokenProvider: () async {
          n++;
          return 't$n';
        },
      );
      ws.connect();
      final first = await server.connection(1);
      await eventually(() => ws.state == ConnectionStatus.open);
      await first.close(1011);
      final second = await server.connection(2);
      await eventually(() {
        try {
          read(second);
          return true;
        } on Object {
          return false;
        }
      });
      expect(read(first), 't1');
      expect(read(second), 't2');
    }

    test('query parameter', () async {
      await rotate(
        const TokenAuth.queryParameter(name: 'access'),
        (c) => c.uri.queryParameters['access']!,
      );
    });

    test('query parameter keeps existing query parameters', () async {
      final ws = make(
        codec: const JsonMessageCodec<Object?>(),
        url: server.url.replace(queryParameters: {'room': '7'}),
        tokenProvider: () => 'abc',
      );
      ws.connect();
      final conn = await server.connection(1);
      expect(conn.uri.queryParameters, {'room': '7', 'token': 'abc'});
    });

    test('header (dart:io)', () async {
      await rotate(
        const TokenAuth.header(),
        (c) => c.headers.value('authorization')!.replaceFirst('Bearer ', ''),
      );
    });

    test('first message', () async {
      await rotate(
        TokenAuth.firstMessage((t) => jsonEncode({'type': 'auth', 'token': t})),
        (c) => (c.messages.first! as Map)['token'] as String,
      );
    });

    test('first message precedes subscriptions and buffered sends', () async {
      final ws = make(
        codec: const JsonMessageCodec<Object?>(),
        tokenAuth:
            TokenAuth.firstMessage((t) => jsonEncode({'type': 'auth', 't': t})),
        tokenProvider: () => 'x',
      );
      ws
        ..subscribe('a')
        ..send({'n': 1});
      ws.connect();
      final conn = await server.connection(1);
      await eventually(() => conn.messages.length >= 3);
      expect(conn.messages, [
        {'type': 'auth', 't': 'x'},
        {'op': 'sub', 'topic': 'a'},
        {'n': 1},
      ]);
    });

    test('null token connects without credentials', () async {
      final ws = make(
        codec: const JsonMessageCodec<Object?>(),
        tokenProvider: () => null,
      );
      ws.connect();
      final conn = await server.connection(1);
      expect(conn.uri.queryParameters, isEmpty);
    });

    test('throwing token provider is retried with backoff', () async {
      var calls = 0;
      final errors = <Object>[];
      final ws = make(
        codec: const JsonMessageCodec<Object?>(),
        tokenProvider: () {
          calls++;
          if (calls < 3) throw StateError('refresh failed');
          return 'ok';
        },
      );
      ws.errors.listen(errors.add);
      ws.connect();
      final conn = await server.connection(1);
      expect(conn.uri.queryParameters['token'], 'ok');
      expect(errors.whereType<StateError>(), hasLength(2));
    });
  });

  group('heartbeat', () {
    test('a missing pong triggers a reconnect', () async {
      server.respondToPings = false;
      final errors = <Object>[];
      final ws = make(
        codec: const JsonMessageCodec<Object?>(),
        heartbeat: const Heartbeat(
          interval: Duration(milliseconds: 40),
          timeout: Duration(milliseconds: 40),
        ),
      );
      ws.errors.listen(errors.add);
      ws.connect();
      final first = await server.connection(1);
      await server.connection(2);
      expect(first.received.map((r) => jsonDecode(r! as String)),
          contains(equals({'type': 'ping'})));
      expect(errors.whereType<HeartbeatTimeoutException>(), isNotEmpty);
    });

    test('pongs keep the connection and are not delivered', () async {
      final received = <Object?>[];
      final ws = make(
        codec: const JsonMessageCodec<Object?>(),
        heartbeat: const Heartbeat(
          interval: Duration(milliseconds: 30),
          timeout: Duration(milliseconds: 60),
        ),
      );
      ws.messages.listen(received.add);
      ws.connect();
      final first = await server.connection(1);
      await Future<void>.delayed(const Duration(milliseconds: 400));
      expect(server.connections, hasLength(1));
      expect(first.received.length, greaterThan(3));
      expect(received, isEmpty);
      expect(ws.state, ConnectionStatus.open);
    });
  });

  group('outgoing buffer', () {
    test('messages sent while disconnected are flushed in order', () async {
      final ws = make(codec: const JsonMessageCodec<Object?>());
      ws.send({'n': 1});
      ws.connect();
      final first = await server.connection(1);
      await eventually(() => first.messages.isNotEmpty);
      expect(first.messages, [
        {'n': 1},
      ]);

      await first.close(1011);
      await eventually(() => ws.state != ConnectionStatus.open);
      ws
        ..send({'n': 2})
        ..send({'n': 3});
      final second = await server.connection(2);
      await eventually(() => second.messages.length >= 2);
      expect(second.messages, [
        {'n': 2},
        {'n': 3},
      ]);
    });

    test('dropOldest keeps the newest messages', () async {
      final ws = make(
        codec: const JsonMessageCodec<Object?>(),
        outboxCapacity: 2,
      );
      for (var i = 1; i <= 4; i++) {
        ws.send({'n': i});
      }
      ws.connect();
      final conn = await server.connection(1);
      await eventually(() => conn.messages.length >= 2);
      await Future<void>.delayed(const Duration(milliseconds: 30));
      expect(conn.messages, [
        {'n': 3},
        {'n': 4},
      ]);
    });

    test('dropNewest keeps the oldest messages', () async {
      final ws = make(
        codec: const JsonMessageCodec<Object?>(),
        outboxCapacity: 2,
        overflowPolicy: OverflowPolicy.dropNewest,
      );
      for (var i = 1; i <= 4; i++) {
        ws.send({'n': i});
      }
      ws.connect();
      final conn = await server.connection(1);
      await eventually(() => conn.messages.length >= 2);
      await Future<void>.delayed(const Duration(milliseconds: 30));
      expect(conn.messages, [
        {'n': 1},
        {'n': 2},
      ]);
    });
  });

  group('close codes', () {
    test('policy violation (1008) is permanent by default', () async {
      final errors = <Object>[];
      final ws = make(codec: const JsonMessageCodec<Object?>());
      ws.errors.listen(errors.add);
      ws.connect();
      final conn = await server.connection(1);
      await eventually(() => ws.state == ConnectionStatus.open);
      await conn.close(1008, 'banned');
      await eventually(() => ws.state == ConnectionStatus.closed);
      await Future<void>.delayed(const Duration(milliseconds: 200));
      expect(server.connections, hasLength(1));
      expect(ws.closeCode, 1008);
      expect(ws.closeReason, 'banned');
      final e = errors.whereType<PermanentCloseException>().single;
      expect(e.code, 1008);
      expect(e.reason, 'banned');
    });

    test('4401 and 4403 are permanent by default', () async {
      for (final code in [4401, 4403]) {
        final ws = make(codec: const JsonMessageCodec<Object?>());
        final before = server.connections.length;
        ws.connect();
        final conn = await server.connection(before + 1);
        await eventually(() => ws.state == ConnectionStatus.open);
        await conn.close(code);
        await eventually(() => ws.state == ConnectionStatus.closed);
        expect(ws.closeCode, code);
      }
    });

    test('permanent codes are configurable', () async {
      final ws = make(
        codec: const JsonMessageCodec<Object?>(),
        permanentCloseCodes: {4000},
      );
      ws.connect();
      final first = await server.connection(1);
      await eventually(() => ws.state == ConnectionStatus.open);
      await first.close(1008);
      final second = await server.connection(2);
      await eventually(() => ws.state == ConnectionStatus.open);
      await second.close(4000);
      await eventually(() => ws.state == ConnectionStatus.closed);
    });

    test('maxAttempts gives up on an unreachable server', () async {
      final dead = await ServerSocket.bind(InternetAddress.loopbackIPv4, 0);
      final port = dead.port;
      await dead.close();
      final errors = <Object>[];
      final ws = make(
        codec: const JsonMessageCodec<Object?>(),
        url: Uri.parse('ws://127.0.0.1:$port/ws'),
        maxAttempts: 3,
      );
      ws.errors.listen(errors.add);
      ws.connect();
      await eventually(() => ws.state == ConnectionStatus.closed);
      expect(errors.whereType<RetryLimitExceededException>(), hasLength(1));
    });
  });

  group('close()', () {
    test('closes cleanly without reconnecting', () async {
      final ws = make(codec: const JsonMessageCodec<Object?>());
      final statesDone = ws.states.toList();
      ws.connect();
      final conn = await server.connection(1);
      await eventually(() => ws.state == ConnectionStatus.open);
      await ws.close();
      await conn.done.timeout(const Duration(seconds: 2));
      expect(conn.socket.closeCode, 1000);
      await Future<void>.delayed(const Duration(milliseconds: 200));
      expect(server.connections, hasLength(1));
      expect(ws.state, ConnectionStatus.closed);
      expect(await statesDone, [
        ConnectionStatus.connecting,
        ConnectionStatus.open,
        ConnectionStatus.closed,
      ]);
    });

    test('close during backoff cancels the pending reconnect', () async {
      final ws = make(codec: const JsonMessageCodec<Object?>());
      ws.connect();
      final conn = await server.connection(1);
      await eventually(() => ws.state == ConnectionStatus.open);
      await conn.close(1011);
      await eventually(() => ws.state == ConnectionStatus.reconnecting);
      await ws.close();
      await Future<void>.delayed(const Duration(milliseconds: 200));
      expect(server.connections, hasLength(1));
    });

    test('connect after close throws', () async {
      final ws = make(codec: const JsonMessageCodec<Object?>());
      await ws.close();
      expect(ws.connect, throwsStateError);
      expect(() => ws.send(1), throwsStateError);
    });
  });
}
