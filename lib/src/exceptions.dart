/// Reported on `errors` when the server closed the connection with a code
/// listed as permanent. The client stops and does not reconnect.
class PermanentCloseException implements Exception {
  /// Creates the exception for close [code] and [reason].
  const PermanentCloseException(this.code, this.reason);

  /// The close code sent by the server.
  final int? code;

  /// The close reason sent by the server, if any.
  final String? reason;

  @override
  String toString() => 'PermanentCloseException($code, $reason)';
}

/// Reported on `errors` when no frame arrived within the heartbeat timeout
/// after a ping. The connection is dropped and re-established.
class HeartbeatTimeoutException implements Exception {
  /// Creates the exception for a heartbeat that waited [timeout].
  const HeartbeatTimeoutException(this.timeout);

  /// How long the client waited for a pong.
  final Duration timeout;

  @override
  String toString() => 'HeartbeatTimeoutException(no pong within $timeout)';
}

/// Reported on `errors` when `maxAttempts` consecutive connection attempts
/// failed. The client stops.
class RetryLimitExceededException implements Exception {
  /// Creates the exception after [attempts] failed attempts.
  const RetryLimitExceededException(this.attempts);

  /// Number of consecutive failed attempts.
  final int attempts;

  @override
  String toString() =>
      'RetryLimitExceededException(gave up after $attempts attempts)';
}
