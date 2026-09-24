/// Connection state of a `ResilientWebSocket` or an `SseClient`.
///
/// Only [connecting], [open], [reconnecting] and [closed] are ever emitted on
/// a `states` stream; [idle] is the value of `state` before `connect()`.
enum ConnectionStatus {
  /// Created, `connect()` not yet called.
  idle,

  /// The first connection attempt is in progress.
  connecting,

  /// Connected; messages flow.
  open,

  /// The connection was lost or an attempt failed; waiting for the next
  /// attempt or making it.
  reconnecting,

  /// Closed for good, by `close()` or by a permanent failure. Terminal.
  closed,
}
