@TestOn('browser')
library;

import 'dart:async';

import 'package:resilient_ws/resilient_ws.dart';
import 'package:test/test.dart';

void main() {
  late int port;

  setUpAll(() async {
    final channel = spawnHybridUri('support/hybrid_server.dart');
    port = await channel.stream.first as int;
  });

  test('SSE streams incrementally in the browser and reconnects', () async {
    var n = 0;
    final client = SseClient(
      url: Uri.parse('http://127.0.0.1:$port/events'),
      tokenProvider: () => 'w${++n}',
    );
    final events = <SseEvent>[];
    final times = <DateTime>[];
    client.events.listen((e) {
      events.add(e);
      times.add(DateTime.now());
    });
    client.connect();
    while (events.length < 3) {
      await Future<void>.delayed(const Duration(milliseconds: 10));
    }
    await client.close();
    expect(events.map((e) => e.data).take(2), ['a', 'b']);
    // "a" arrived while the response was still open, 400 ms before "b":
    // the body is streamed, not buffered until the end.
    expect(times[1].difference(times[0]).inMilliseconds, greaterThan(250));
    expect(events[2].data, 'last=2 auth=Bearer w2');
  });

  test('WebSocket with a query-parameter token works in the browser', () async {
    final ws = ResilientWebSocket<Object?>(
      url: Uri.parse('ws://127.0.0.1:$port/ws'),
      codec: const JsonMessageCodec<Object?>(),
      tokenProvider: () => 'tok',
    );
    final first = ws.messages.take(2).toList();
    ws.connect();
    ws.send({'echo': 1});
    expect(await first.timeout(const Duration(seconds: 5)), [
      {'token': 'tok'},
      {'echo': 1},
    ]);
    await ws.close();
  });

  test('header auth is rejected on the web with UnsupportedError', () async {
    final ws = ResilientWebSocket<Object?>(
      url: Uri.parse('ws://127.0.0.1:$port/ws'),
      codec: const JsonMessageCodec<Object?>(),
      tokenProvider: () => 'tok',
      tokenAuth: const TokenAuth.header(),
      maxAttempts: 1,
    );
    final errors = ws.errors.toList();
    ws.connect();
    expect((await errors).first, isA<UnsupportedError>());
  });
}
