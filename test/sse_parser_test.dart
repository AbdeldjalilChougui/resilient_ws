import 'dart:async';

import 'package:resilient_ws/resilient_ws.dart';
import 'package:test/test.dart';

List<SseEvent> parseAll(List<String> chunks, [SseParser? parser]) {
  final p = parser ?? SseParser();
  final out = <SseEvent>[];
  for (final c in chunks) {
    out.addAll(p.add(c));
  }
  p.close();
  return out;
}

void main() {
  group('WHATWG spec examples', () {
    test('multi-line data is joined with LF (stock ticker example)', () {
      final events = parseAll(['data: YHOO\ndata: +2\ndata: 10\n\n']);
      expect(events, hasLength(1));
      expect(events.single.type, 'message');
      expect(events.single.data, 'YHOO\n+2\n10');
    });

    test('comments, id, id reset, one-space stripping, no final dispatch', () {
      final p = SseParser();
      final events = parseAll([
        ': test stream\n'
            '\n'
            'data: first event\n'
            'id: 1\n'
            '\n'
            'data:second event\n'
            'id\n'
            '\n'
            'data:  third event\n',
      ], p);
      expect(events.map((e) => e.data), ['first event', 'second event']);
      expect(events[0].lastEventId, '1');
      // A bare "id" field resets the last event ID to the empty string.
      expect(events[1].lastEventId, '');
      expect(p.lastEventId, '');
    });

    test('third block is only dispatched once a blank line follows', () {
      final events = parseAll(['data:  third event\n\n']);
      // Only one leading space is removed.
      expect(events.single.data, ' third event');
    });

    test('empty data fields: "", "\\n", and a trailing undispatched block', () {
      final events = parseAll(['data\n\ndata\ndata\n\ndata:']);
      expect(events.map((e) => e.data), ['', '\n']);
    });

    test('"data:test" and "data: test" are identical', () {
      final events = parseAll(['data:test\n\ndata: test\n\n']);
      expect(events.map((e) => e.data), ['test', 'test']);
    });

    test('event field sets the type (named events example)', () {
      final events = parseAll([
        'event: add\ndata: 73857293\n\n'
            'event: remove\ndata: 2153\n\n'
            'event: add\ndata: 113411\n\n',
      ]);
      expect(events.map((e) => e.type), ['add', 'remove', 'add']);
      expect(events.map((e) => e.data), ['73857293', '2153', '113411']);
    });
  });

  group('line endings', () {
    for (final entry in {'LF': '\n', 'CR': '\r', 'CRLF': '\r\n'}.entries) {
      test('${entry.key} endings', () {
        final nl = entry.value;
        final events = parseAll(['data: a${nl}data: b$nl${nl}data: c$nl$nl']);
        expect(events.map((e) => e.data), ['a\nb', 'c']);
      });
    }

    test('CRLF split across chunks is a single line break', () {
      final events = parseAll(['data: a\r', '\n', '\r', '\ndata: b\r\n\r\n']);
      expect(events.map((e) => e.data), ['a', 'b']);
    });

    test('lines split at arbitrary chunk boundaries', () {
      const stream = 'event: x\r\ndata: hello\r\nid: 42\r\n\r\ndata: bye\n\n';
      for (var size = 1; size < stream.length; size++) {
        final chunks = <String>[
          for (var i = 0; i < stream.length; i += size)
            stream.substring(i, (i + size).clamp(0, stream.length)),
        ];
        final events = parseAll(chunks);
        expect(events.map((e) => e.data), ['hello', 'bye'],
            reason: 'chunk size $size');
        expect(events.first.type, 'x');
        expect(events.first.lastEventId, '42');
        expect(events.last.type, 'message');
        expect(events.last.lastEventId, '42');
      }
    });
  });

  group('BOM', () {
    test('one leading BOM is ignored', () {
      final events = parseAll(['\uFEFFdata: x\n\n']);
      expect(events.single.data, 'x');
    });

    test('BOM arriving in its own chunk after an empty chunk', () {
      final events = parseAll(['', '\uFEFF', 'data: x\n\n']);
      expect(events.single.data, 'x');
    });

    test('only the first BOM is stripped', () {
      final events = parseAll(['\uFEFF\uFEFFdata: x\n\ndata: y\n\n']);
      // The second BOM becomes part of the field name, so that line is ignored.
      expect(events.map((e) => e.data), ['y']);
    });

    test('a BOM later in the stream is data', () {
      final events = parseAll(['data: \uFEFFx\n\n']);
      expect(events.single.data, '\uFEFFx');
    });
  });

  group('fields', () {
    test('comments and heartbeats produce no events', () {
      final events = parseAll([':\n\n: keep-alive\n\n:ping\n\n']);
      expect(events, isEmpty);
    });

    test('unknown fields are ignored', () {
      final events = parseAll(['foo: bar\ndata: x\nbaz\n\n']);
      expect(events.single.data, 'x');
    });

    test('field names are case-sensitive', () {
      final events = parseAll(['Data: x\nDATA: y\n\n']);
      expect(events, isEmpty);
    });

    test('value may contain colons', () {
      final events = parseAll(['data: a:b: c\n\n']);
      expect(events.single.data, 'a:b: c');
    });

    test('event type resets after dispatch', () {
      final events = parseAll(['event: a\ndata: 1\n\ndata: 2\n\n']);
      expect(events.map((e) => e.type), ['a', 'message']);
    });

    test('event type set without data is reset by the blank line', () {
      final events = parseAll(['event: a\n\ndata: 2\n\n']);
      expect(events.single.type, 'message');
    });

    test('id persists across events until changed', () {
      final events =
          parseAll(['id: 7\ndata: a\n\ndata: b\n\nid: 8\ndata: c\n\n']);
      expect(events.map((e) => e.lastEventId), ['7', '7', '8']);
    });

    test('id is updated even when the block has no data', () {
      final p = SseParser();
      final events = parseAll(['id: 5\n\n'], p);
      expect(events, isEmpty);
      expect(p.lastEventId, '5');
    });

    test('id containing NULL is ignored', () {
      final p = SseParser();
      parseAll(['id: 1\n\nid: a\u0000b\n\n'], p);
      expect(p.lastEventId, '1');
    });

    test('retry with ASCII digits sets reconnection time', () {
      final p = SseParser();
      final retries = <Duration>[];
      p.onRetry = retries.add;
      parseAll(['retry: 2500\n\n'], p);
      expect(p.retry, const Duration(milliseconds: 2500));
      expect(retries, [const Duration(milliseconds: 2500)]);
    });

    test('invalid retry values are ignored', () {
      final p = SseParser();
      parseAll(
          ['retry: 10\n\nretry: 1.5\nretry: -3\nretry: abc\nretry:\n\n'], p);
      expect(p.retry, const Duration(milliseconds: 10));
    });

    test('incomplete event at end of stream is discarded', () {
      final p = SseParser();
      expect(p.add('data: partial\n'), isEmpty);
      p.close();
      expect(p.add('\n'), isEmpty);
    });
  });

  test('SseParser.transformer works as a StreamTransformer', () async {
    final events = await Stream.fromIterable(['data: a\n', '\ndata: b\n\n'])
        .transform(SseParser.transformer())
        .toList();
    expect(events.map((e) => e.data), ['a', 'b']);
  });

  test('SseEvent value semantics', () {
    const a = SseEvent(data: 'x', type: 't', lastEventId: '1');
    const b = SseEvent(data: 'x', type: 't', lastEventId: '1');
    expect(a, b);
    expect(a.hashCode, b.hashCode);
    expect(a.toString(), contains('x'));
  });

  test('a stream-level error is forwarded by the transformer', () async {
    final ctrl = StreamController<String>();
    final done = expectLater(
        ctrl.stream.transform(SseParser.transformer()).toList(),
        throwsStateError);
    ctrl.addError(StateError('boom'));
    await ctrl.close();
    await done;
  });
}
