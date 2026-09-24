import 'package:web_socket_channel/io.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

/// Default connector on `dart:io` platforms; supports headers.
WebSocketChannel platformConnect(
  Uri url, {
  required Map<String, String> headers,
  Iterable<String>? protocols,
}) =>
    IOWebSocketChannel.connect(url, headers: headers, protocols: protocols);
