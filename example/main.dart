// Connects a ResilientWebSocket and an SseClient to a local demo server
// started by this file, then prints what arrives.
//
// Run with: dart run example/main.dart
import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:resilient_ws/resilient_ws.dart';

/// An example message type.
class Tick {
  Tick(this.topic, this.value);

  factory Tick.fromJson(Object? json) {
    final map = json! as Map<String, Object?>;
    return Tick(map['topic']! as String, map['value']! as int);
  }

  final String topic;
  final int value;

  Map<String, Object?> toJson() => {'topic': topic, 'value': value};
}

Future<void> main() async {
  final server = await _demoServer();
  final base = 'localhost:${server.port}';

  var refreshes = 0;
  Future<String> token() async => 'token-${++refreshes}';

  // WebSocket ---------------------------------------------------------------
  final ws = ResilientWebSocket<Tick>(
    url: Uri.parse('ws://$base/ws'),
    codec: JsonMessageCodec<Tick>(
      fromJson: Tick.fromJson,
      toJson: (t) => t.toJson(),
    ),
    tokenProvider: token,
    tokenAuth: const TokenAuth.header(),
    heartbeat: const Heartbeat(interval: Duration(seconds: 5)),
  );
  ws.states.listen((s) => print('ws state: ${s.name}'));
  ws.messages.listen((t) => print('ws tick: ${t.topic} = ${t.value}'));
  ws.subscribe('prices');
  ws.connect();

  // Server-Sent Events -------------------------------------------------------
  final sse = SseClient(url: Uri.parse('http://$base/events'));
  sse.events.listen((e) => print('sse ${e.type} #${e.lastEventId}: ${e.data}'));
  sse.connect();

  await Future<void>.delayed(const Duration(seconds: 2));
  await ws.close();
  await sse.close();
  await server.close(force: true);
}

/// A tiny server: the WebSocket echoes a tick per subscription, and the
/// event stream sends two events.
Future<HttpServer> _demoServer() async {
  final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
  server.listen((req) async {
    if (req.uri.path == '/ws') {
      final socket = await WebSocketTransformer.upgrade(req);
      socket.listen((frame) {
        final msg = jsonDecode(frame as String) as Map<String, Object?>;
        if (msg['type'] == 'ping') {
          socket.add(jsonEncode({'type': 'pong'}));
        } else if (msg['op'] == 'sub') {
          socket.add(jsonEncode({'topic': msg['topic'], 'value': 42}));
        }
      });
    } else {
      req.response
        ..headers.contentType = ContentType('text', 'event-stream')
        ..bufferOutput = false
        ..write(': hello\n\nid: 1\nevent: greeting\ndata: hi\n\n'
            'id: 2\ndata: line one\ndata: line two\n\n');
      await req.response.flush();
    }
  });
  return server;
}
