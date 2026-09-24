import 'dart:collection';

/// What a full [Outbox] does with one more message.
enum OverflowPolicy {
  /// Evict the oldest queued message to make room. Suits state updates where
  /// only recent messages matter.
  dropOldest,

  /// Reject the incoming message and keep the queue as it is. Suits flows
  /// where the first messages matter most.
  dropNewest,
}

/// A bounded FIFO queue for messages sent while disconnected.
class Outbox<T> {
  /// Creates a queue holding at most [capacity] messages.
  ///
  /// [onDrop] is called with every message that is discarded because the
  /// queue is full. A [capacity] of 0 disables buffering.
  Outbox({
    this.capacity = 100,
    this.policy = OverflowPolicy.dropOldest,
    this.onDrop,
  }) {
    if (capacity < 0) {
      throw ArgumentError.value(capacity, 'capacity', 'must not be negative');
    }
  }

  /// Maximum number of queued messages.
  final int capacity;

  /// What happens when a message arrives and the queue is full.
  final OverflowPolicy policy;

  /// Called with each message that is discarded.
  final void Function(T message)? onDrop;

  final Queue<T> _queue = Queue<T>();

  /// Number of queued messages.
  int get length => _queue.length;

  /// Whether the queue is empty.
  bool get isEmpty => _queue.isEmpty;

  /// Queues [message]. Returns false if [message] itself was dropped.
  bool add(T message) {
    if (capacity == 0) {
      onDrop?.call(message);
      return false;
    }
    if (_queue.length >= capacity) {
      if (policy == OverflowPolicy.dropNewest) {
        onDrop?.call(message);
        return false;
      }
      final evicted = _queue.removeFirst();
      onDrop?.call(evicted);
    }
    _queue.addLast(message);
    return true;
  }

  /// Removes and returns every queued message, oldest first.
  List<T> drain() {
    final all = _queue.toList();
    _queue.clear();
    return all;
  }
}
