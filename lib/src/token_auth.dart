import 'dart:async';

/// Returns the current access token, or null to connect without one.
///
/// Called again before every connection attempt, so a refreshed token is
/// always used on reconnect.
typedef TokenProvider = FutureOr<String?> Function();

/// How a WebSocket token reaches the server.
sealed class TokenAuth {
  const TokenAuth();

  /// Sends the token as an HTTP header on the upgrade request, for example
  /// `Authorization: Bearer <token>`.
  ///
  /// Only available where `dart:io` is. Browsers do not let WebSockets set
  /// headers, so on the web connecting throws [UnsupportedError].
  const factory TokenAuth.header({String name, String prefix}) =
      HeaderTokenAuth;

  /// Appends the token to the URL as a query parameter, `?token=<token>` by
  /// default. Works on every platform, but URLs can end up in server logs.
  const factory TokenAuth.queryParameter({String name}) = QueryTokenAuth;

  /// Sends the frame built by [build] as the first message once the socket
  /// is open, before subscriptions and buffered messages.
  const factory TokenAuth.firstMessage(Object Function(String token) build) =
      FirstMessageTokenAuth;
}

/// Token in an HTTP header. See [TokenAuth.header].
final class HeaderTokenAuth extends TokenAuth {
  /// Creates header auth.
  const HeaderTokenAuth({this.name = 'Authorization', this.prefix = 'Bearer '});

  /// Header name.
  final String name;

  /// Text placed before the token in the header value.
  final String prefix;
}

/// Token in a query parameter. See [TokenAuth.queryParameter].
final class QueryTokenAuth extends TokenAuth {
  /// Creates query parameter auth.
  const QueryTokenAuth({this.name = 'token'});

  /// Query parameter name.
  final String name;
}

/// Token in the first message. See [TokenAuth.firstMessage].
final class FirstMessageTokenAuth extends TokenAuth {
  /// Creates first-message auth.
  const FirstMessageTokenAuth(this.build);

  /// Builds the frame (a `String` or `List<int>`) carrying the token.
  final Object Function(String token) build;
}
