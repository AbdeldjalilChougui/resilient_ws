# resilient_ws

Realtime clients for Dart and Flutter that keep working on bad mobile networks:
a WebSocket client and a Server-Sent Events client. Both reconnect on their own,
fetch a fresh token for every connection, and stop cleanly when told to.

[![pub package](https://img.shields.io/pub/v/resilient_ws.svg)](https://pub.dev/packages/resilient_ws)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

## Features

**`ResilientWebSocket<T>`**

- States `connecting`, `open`, `reconnecting` and `closed`, available as a stream
  and as a getter.
- Exponential backoff with jitter and a cap (1 s doubling to 30 s by default).
  The backoff resets only after a connection has stayed up for `stableAfter`
  (10 s by default), so a server that accepts and then drops connections does
  not get hammered.
- The token provider runs before every connection attempt. Three ways to send
  the token: an HTTP header (`dart:io` only), a query parameter, or a first
  message.
- An application-level heartbeat. If no frame arrives within `timeout` of a
  ping, the client reconnects. Any inbound frame counts as proof of life, and
  pongs are not passed on to `messages`.
- A bounded buffer for messages sent while disconnected, with `dropOldest` or
  `dropNewest` overflow. Buffered messages are flushed in order after a
  reconnect.
- Topic subscriptions are re-sent after every reconnect. The subscribe and
  unsubscribe frames come from a pluggable `SubscriptionCodec`.
- Messages are typed through a pluggable `MessageCodec<T>`, with
  `JsonMessageCodec` included.
- Close codes that mean "stop" (1008, 4401 and 4403 by default; configurable)
  end the client instead of starting a retry loop.
- An optional `Stream<bool>` of network availability pauses reconnects while
  offline and reconnects at once when the network returns.
- `close()` is clean: it sends a close frame, cancels every timer, and never
  triggers a reconnect.

**`SseClient`**

- The parser follows the WHATWG HTML "Server-sent events" section:
  - the `event`, `data` (multi-line), `id` and `retry` fields;
  - comments and heartbeats;
  - a leading BOM;
  - CRLF, CR and LF line endings, including a CRLF split across chunks;
  - an event is dispatched only on a blank line.
- It reconnects automatically, sends `Last-Event-ID`, and honours the server's
  `retry:` field. Consecutive failures double the wait, up to `maxRetry`.
- The token provider runs before every request.
- HTTP 204 stops the client, as the spec says. By default 4xx statuses stop
  it too, while 5xx, 408 and 429 are retried. Both rules are configurable.
- An optional `idleTimeout` reconnects when a connection goes silent.
- The `http.Client` can be injected.

Both clients work on every platform, including the web.

## Install

```sh
dart pub add resilient_ws
```

## Usage

### WebSocket

```dart
import 'package:resilient_ws/resilient_ws.dart';

class Tick {
  Tick(this.topic, this.value);
  factory Tick.fromJson(Object? json) {
    final map = json! as Map<String, Object?>;
    return Tick(map['topic']! as String, map['value']! as num);
  }
  final String topic;
  final num value;
  Map<String, Object?> toJson() => {'topic': topic, 'value': value};
}

Future<String?> readAccessToken() async => 'token'; // your auth layer

Future<void> main() async {
  final ws = ResilientWebSocket<Tick>(
    url: Uri.parse('wss://api.example.com/ws'),
    codec: JsonMessageCodec<Tick>(
      fromJson: Tick.fromJson,
      toJson: (t) => t.toJson(),
    ),
    tokenProvider: readAccessToken, // called before every connect
    tokenAuth: const TokenAuth.queryParameter(name: 'token'),
    heartbeat: const Heartbeat(
      interval: Duration(seconds: 25),
      timeout: Duration(seconds: 10),
    ),
    outboxCapacity: 50,
    overflowPolicy: OverflowPolicy.dropOldest,
    permanentCloseCodes: {1008, 4001, 4003},
    // networkAvailable: connectivityStream.map((r) => r != offline),
  );

  ws.states.listen((s) => print('state: ${s.name}'));
  ws.messages.listen((tick) => print('${tick.topic}: ${tick.value}'));
  ws.errors.listen((e) {
    if (e is PermanentCloseException) print('server said stop: ${e.code}');
  });

  ws.subscribe('prices'); // re-sent after every reconnect
  ws.connect();
  ws.send(Tick('hello', 1)); // buffered until the socket is open

  await Future<void>.delayed(const Duration(seconds: 30));
  await ws.close(); // no reconnect follows
}
```

The default subscription frames are `{"op":"sub","topic":"prices"}` and
`{"op":"unsub","topic":"prices"}`. You can supply your own protocol:

```dart
import 'dart:convert';

import 'package:resilient_ws/resilient_ws.dart';

final ws = ResilientWebSocket<Object?>(
  url: Uri.parse('wss://api.example.com/cable'),
  codec: const JsonMessageCodec<Object?>(),
  subscriptionCodec: SubscriptionCodec.from(
    subscribe: (t) => jsonEncode({'command': 'subscribe', 'channel': t}),
    unsubscribe: (t) => jsonEncode({'command': 'unsubscribe', 'channel': t}),
  ),
  tokenAuth: TokenAuth.firstMessage(
    (token) => jsonEncode({'type': 'auth', 'token': token}),
  ),
  tokenProvider: () => 'token',
);
```

When the token goes in the first message, the client sends it before the
subscriptions and before any buffered messages.

### Server-Sent Events

```dart
import 'package:resilient_ws/resilient_ws.dart';

Future<void> main() async {
  final sse = SseClient(
    url: Uri.parse('https://api.example.com/events'),
    tokenProvider: () async => 'token', // "Authorization: Bearer token"
    retry: const Duration(seconds: 3), // until the server sends "retry:"
    idleTimeout: const Duration(seconds: 60), // server heartbeats every 15 s
    // client: myHttpClient, // optional, not closed by close()
  );

  sse.events.listen((e) => print('${e.type} #${e.lastEventId}: ${e.data}'));
  sse.states.listen((s) => print('state: ${s.name}'));
  sse.connect();

  await Future<void>.delayed(const Duration(minutes: 1));
  await sse.close();
}
```

You can also use the parser by itself:

```dart
import 'dart:convert';

import 'package:resilient_ws/resilient_ws.dart';

Stream<SseEvent> parse(Stream<List<int>> bytes) => bytes
    .transform(const Utf8Decoder(allowMalformed: true))
    .transform(SseParser.transformer());
```

## Behaviour details

- **Connection order.** On every connect the client sends the first-message
  token (if used), then every subscription, then the buffered messages.
- **Backoff.** The delay for attempt `n` is `min(max, initial * multiplier^n)`,
  minus a random share of up to `jitter` (0.2 by default). Jitter only shortens
  a delay, so `max` is a hard cap.
- **Heartbeat.** A ping is sent every `interval`. A timeout timer starts at each
  ping and is cancelled by any inbound frame. When it fires, the client emits
  `HeartbeatTimeoutException` on `errors`, drops the socket and reconnects.
- **Failures.** A throwing token provider, a handshake that fails or takes
  longer than `connectTimeout`, or a dropped connection are all retried.
  `maxAttempts` (off by default) stops the client after that many failed
  attempts in a row.
- **Stale connections.** Every connection attempt has a generation number, so
  frames, errors and close events from a replaced connection are ignored.
  `reconnect()` forces a fresh connection, which is useful when an app returns
  to the foreground.
- **SSE reconnection.** After a stream ends, the client waits the reconnection
  time and reconnects. The reconnection time is the server's `retry:` value,
  or `retry` if the server sent none. Consecutive failures (network errors,
  retried statuses) wait `retry`, `2 × retry`, `4 × retry` and so on, up to
  `maxRetry`. A server `retry:` above `maxRetry` is still honoured. An `id:`
  field counts only once the blank line after it arrives, as the spec says, so
  a half-received event never moves `Last-Event-ID`.
- **SSE content type.** A 200 response that is not `text/event-stream` fails
  the connection for good, as the spec says.

## Limitations

- **Header auth is `dart:io` only.** Browsers do not let a WebSocket set
  headers. On the web, `TokenAuth.header` (or `headers:`) makes the connection
  attempt fail with `UnsupportedError`. Use `TokenAuth.queryParameter` or
  `TokenAuth.firstMessage` instead.
- **SSE on the web.** The client uses `package:http`, not the browser's
  `EventSource`, so it can send `Authorization` and `Last-Event-ID` headers.
  From version 1.3, `http.Client()` on the web is the fetch-based
  `BrowserClient`, which streams the response body. This package requires
  `http` ^1.6.0, which also fixes cancellation of web response streams. The
  browser test in `test/web_integration_test.dart` checks that events arrive
  one by one in Chrome and that the client reconnects with `Last-Event-ID`.
  Two caveats remain:
  - Custom headers trigger a CORS preflight, so the server must allow
    `authorization`, `last-event-id` and `cache-control`.
  - A proxy or dev server that buffers or compresses `text/event-stream` can
    still delay events. That is outside the client's control.
- **The heartbeat is application-level.** The server must answer your ping
  frame, or send any traffic within `timeout`. WebSocket protocol-level pings
  are not configured.
- **Handshake rejections cannot be told apart.** A server that rejects the
  upgrade with HTTP 401 or 403 looks like any other failed handshake to
  `web_socket_channel`, so it is retried with backoff (with a fresh token each
  time). Use `maxAttempts` to bound it, or have the server accept the upgrade
  and close with an auth close code, such as 4401.
- **Undispatched SSE data is discarded.** An event cut off mid-stream is thrown
  away, as the spec requires.

## Specification

The SSE parser and the reconnect rules follow the WHATWG HTML Living Standard,
section 9.2 "Server-sent events"
(<https://html.spec.whatwg.org/multipage/server-sent-events.html>). The parser
tests include the spec's own examples. WebSocket close codes follow RFC 6455
section 7.4.

## License

MIT. See [LICENSE](LICENSE).

Made by [Abdeldjalil Chougui](https://abdeldjalilchougui.biz/).
