import 'package:web_socket_channel/web_socket_channel.dart';

/// Opens a [WebSocketChannel]. Injectable for tests or custom transports.
///
/// [headers] is non-empty only when [HeaderTokenAuth] or extra headers are
/// used; implementations that cannot send headers should throw
/// [UnsupportedError] in that case.
typedef WebSocketConnector = WebSocketChannel Function(
  Uri url, {
  required Map<String, String> headers,
  Iterable<String>? protocols,
});
