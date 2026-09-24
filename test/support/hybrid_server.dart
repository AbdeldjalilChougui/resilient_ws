import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:stream_channel/stream_channel.dart';

/// VM-side server for the browser tests, started with `spawnHybridUri`.
Future<void> hybridMain(StreamChannel<Object?> channel) async {
  final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
  var sseRequests = 0;
  server.listen((req) async {
    final res = req.response;
    res.headers
      ..set('Access-Control-Allow-Origin', '*')
      ..set('Access-Control-Allow-Headers',
          'authorization, last-event-id, cache-control, accept');
    if (req.method == 'OPTIONS') {
      res.statusCode = 204;
      await res.close();
      return;
    }
    if (req.uri.path == '/ws' && WebSocketTransformer.isUpgradeRequest(req)) {
      final ws = await WebSocketTransformer.upgrade(req);
      ws.add(jsonEncode({'token': req.uri.queryParameters['token']}));
      ws.listen(ws.add);
      return;
    }
    if (req.uri.path == '/events') {
      sseRequests++;
      res
        ..statusCode = 200
        ..headers.set('Content-Type', 'text/event-stream')
        ..bufferOutput = false;
      if (sseRequests == 1) {
        res.write('retry: 50\nid: 1\ndata: a\n\n');
        await res.flush();
        await Future<void>.delayed(const Duration(milliseconds: 400));
        res.write('id: 2\ndata: b\n\n');
        await res.flush();
        await res.close();
      } else {
        final auth = req.headers.value('authorization');
        res.write('data: last=${req.headers.value('last-event-id')} '
            'auth=$auth\n\n');
        await res.flush();
      }
      return;
    }
    res.statusCode = 404;
    await res.close();
  });
  channel.sink.add(server.port);
}
