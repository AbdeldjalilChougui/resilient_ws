/// One event dispatched from a `text/event-stream`.
///
/// Mirrors the `MessageEvent` a browser `EventSource` fires: [type] is the
/// value of the last `event:` field (or `message`), [data] is every `data:`
/// line joined with `\n`, and [lastEventId] is the last `id:` seen on the
/// stream so far, which persists across events.
class SseEvent {
  /// Creates an event. [type] defaults to `message`.
  const SseEvent({
    required this.data,
    this.type = 'message',
    this.lastEventId = '',
  });

  /// The event type, from the `event:` field. Defaults to `message`.
  final String type;

  /// The event payload: all `data:` lines joined with a line feed.
  final String data;

  /// The last event ID in effect when this event was dispatched.
  final String lastEventId;

  @override
  bool operator ==(Object other) =>
      other is SseEvent &&
      other.type == type &&
      other.data == data &&
      other.lastEventId == lastEventId;

  @override
  int get hashCode => Object.hash(type, data, lastEventId);

  @override
  String toString() =>
      'SseEvent(type: $type, lastEventId: $lastEventId, data: $data)';
}
