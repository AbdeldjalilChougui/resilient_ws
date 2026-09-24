import 'package:web_socket_channel/web_socket_channel.dart';

/// Default connector where `dart:io` is unavailable (browsers).
WebSocketChannel platformConnect(
  Uri url, {
  required Map<String, String> headers,
  Iterable<String>? protocols,
}) {
  if (headers.isNotEmpty) {
    throw UnsupportedError(
      'Browsers cannot set WebSocket headers. Use TokenAuth.queryParameter '
      'or TokenAuth.firstMessage on the web.',
    );
  }
  return WebSocketChannel.connect(url, protocols: protocols);
}
