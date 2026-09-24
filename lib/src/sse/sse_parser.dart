import 'dart:async';

import 'sse_event.dart';

/// Incremental parser for the `text/event-stream` format.
///
/// Implements the "Interpreting an event stream" algorithm of the WHATWG HTML
/// standard, section 9.2 "Server-sent events":
///
/// * one leading U+FEFF byte order mark is ignored;
/// * lines end in CRLF, a lone LF or a lone CR, including a CRLF split
///   across two chunks;
/// * lines starting with `:` are comments (typically heartbeats);
/// * the `event`, `data`, `id` and `retry` fields are handled, others ignored;
/// * a single space after the colon is removed from the value;
/// * events are dispatched only on a blank line, and an incomplete event at
///   the end of the stream is discarded.
///
/// Feed decoded text with [add]; call [close] at the end of the stream.
class SseParser {
  /// Creates a parser. [lastEventId] seeds the last event ID buffer, which a
  /// reconnecting client carries over from the previous connection.
  SseParser({String lastEventId = ''})
      : _lastEventId = lastEventId,
        _idBuffer = lastEventId;

  /// Returns a [StreamTransformer] that parses decoded text into events.
  ///
  /// [onRetry] is called whenever a valid `retry:` field is parsed.
  static StreamTransformer<String, SseEvent> transformer({
    String lastEventId = '',
    void Function(Duration retry)? onRetry,
  }) =>
      StreamTransformer<String, SseEvent>.fromBind((input) {
        final parser = SseParser(lastEventId: lastEventId)..onRetry = onRetry;
        return input
            .expand(parser.add)
            .transform(StreamTransformer<SseEvent, SseEvent>.fromHandlers(
          handleDone: (sink) {
            parser.close();
            sink.close();
          },
        ));
      });

  /// Called whenever a valid `retry:` field sets a new reconnection time.
  void Function(Duration retry)? onRetry;

  final StringBuffer _line = StringBuffer();
  final StringBuffer _data = StringBuffer();
  String _eventType = '';
  String _lastEventId;
  String _idBuffer;
  Duration? _retry;
  bool _started = false;
  bool _pendingCr = false;
  bool _closed = false;

  /// The event source's last event ID string: what a reconnecting client
  /// sends as `Last-Event-ID`.
  ///
  /// As in the standard, an `id:` field takes effect at the blank line that
  /// ends its block, even when the block carries no data, and never if the
  /// stream ends before that blank line.
  String get lastEventId => _lastEventId;

  /// The reconnection time most recently sent with a `retry:` field, or null
  /// if the server never sent a valid one.
  Duration? get retry => _retry;

  /// Parses [chunk] and returns the events completed by it, in order.
  ///
  /// Returns an empty list once [close] has been called.
  List<SseEvent> add(String chunk) {
    if (_closed || chunk.isEmpty) return const [];
    var start = 0;
    if (!_started) {
      _started = true;
      if (chunk.codeUnitAt(0) == 0xFEFF) start = 1;
    }
    final out = <SseEvent>[];
    var segment = start;
    for (var i = start; i < chunk.length; i++) {
      final c = chunk.codeUnitAt(i);
      if (_pendingCr) {
        _pendingCr = false;
        if (c == 0x0A) {
          // Second half of a CRLF whose CR ended the previous chunk.
          segment = i + 1;
          continue;
        }
      }
      if (c == 0x0A || c == 0x0D) {
        _line.write(chunk.substring(segment, i));
        _processLine(_line.toString(), out);
        _line.clear();
        if (c == 0x0D) {
          if (i + 1 < chunk.length) {
            if (chunk.codeUnitAt(i + 1) == 0x0A) i++;
          } else {
            _pendingCr = true;
          }
        }
        segment = i + 1;
      }
    }
    if (segment < chunk.length) _line.write(chunk.substring(segment));
    return out;
  }

  /// Signals the end of the stream. Any partially received event is
  /// discarded, as the standard requires.
  void close() {
    _closed = true;
    _line.clear();
    _data.clear();
    _eventType = '';
  }

  void _processLine(String line, List<SseEvent> out) {
    if (line.isEmpty) {
      _dispatch(out);
      return;
    }
    if (line.codeUnitAt(0) == 0x3A) return; // ':' comment
    final colon = line.indexOf(':');
    final String field;
    var value = '';
    if (colon >= 0) {
      field = line.substring(0, colon);
      value = line.substring(colon + 1);
      if (value.isNotEmpty && value.codeUnitAt(0) == 0x20) {
        value = value.substring(1);
      }
    } else {
      field = line;
    }
    switch (field) {
      case 'event':
        _eventType = value;
      case 'data':
        _data
          ..write(value)
          ..write('\n');
      case 'id':
        if (!value.contains('\u0000')) _idBuffer = value;
      case 'retry':
        if (value.isNotEmpty && _allAsciiDigits(value)) {
          final ms = int.tryParse(value);
          if (ms != null) {
            _retry = Duration(milliseconds: ms);
            onRetry?.call(_retry!);
          }
        }
    }
  }

  void _dispatch(List<SseEvent> out) {
    _lastEventId = _idBuffer;
    if (_data.isEmpty) {
      _eventType = '';
      return;
    }
    var data = _data.toString();
    data = data.substring(0, data.length - 1); // trailing LF
    out.add(SseEvent(
      type: _eventType.isEmpty ? 'message' : _eventType,
      data: data,
      lastEventId: _lastEventId,
    ));
    _data.clear();
    _eventType = '';
  }

  static bool _allAsciiDigits(String s) {
    for (var i = 0; i < s.length; i++) {
      final c = s.codeUnitAt(i);
      if (c < 0x30 || c > 0x39) return false;
    }
    return true;
  }
}
