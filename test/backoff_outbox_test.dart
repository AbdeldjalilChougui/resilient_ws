import 'dart:math';

import 'package:resilient_ws/resilient_ws.dart';
import 'package:test/test.dart';

/// A [Random] that always returns the same double.
class FixedRandom implements Random {
  FixedRandom(this.value);
  final double value;
  @override
  double nextDouble() => value;
  @override
  int nextInt(int max) => (value * max).floor();
  @override
  bool nextBool() => value >= 0.5;
}

void main() {
  group('Backoff', () {
    test('grows exponentially and is capped, without jitter', () {
      final b = Backoff(
        initial: Duration(seconds: 1),
        max: Duration(seconds: 30),
        jitter: 0,
      );
      expect([for (var i = 0; i < 8; i++) b.delay(i).inSeconds],
          [1, 2, 4, 8, 16, 30, 30, 30]);
    });

    test('custom multiplier', () {
      final b = Backoff(
        initial: Duration(milliseconds: 100),
        max: Duration(seconds: 10),
        multiplier: 3,
        jitter: 0,
      );
      expect([for (var i = 0; i < 4; i++) b.delay(i).inMilliseconds],
          [100, 300, 900, 2700]);
    });

    test('huge attempt numbers do not overflow', () {
      final b = Backoff(jitter: 0);
      expect(b.delay(100000), b.max);
    });

    test('jitter subtracts up to jitter * base and never exceeds the cap', () {
      final lo = Backoff(jitter: 0.5, random: FixedRandom(0.999999));
      final hi = Backoff(jitter: 0.5, random: FixedRandom(0));
      // attempt 2 -> base 4 s
      expect(hi.delay(2), const Duration(seconds: 4));
      expect(lo.delay(2).inMilliseconds, closeTo(2000, 1));
      expect(hi.delay(50), const Duration(seconds: 30));
    });

    test('jitter spreads real random delays inside the band', () {
      final b = Backoff(jitter: 0.3, random: Random(1));
      for (var i = 0; i < 200; i++) {
        final d = b.delay(3).inMilliseconds; // base 8000
        expect(d, inInclusiveRange(5600, 8000));
      }
    });

    test('rejects invalid configuration', () {
      expect(() => Backoff(jitter: 1.5), throwsArgumentError);
      expect(() => Backoff(multiplier: 0.5), throwsArgumentError);
      expect(
          () => Backoff(
              initial: const Duration(seconds: 5),
              max: const Duration(seconds: 1)),
          throwsArgumentError);
    });
  });

  group('Outbox', () {
    test('keeps messages in order up to capacity', () {
      final o = Outbox<String>(capacity: 3);
      expect(o.add('a'), isTrue);
      expect(o.add('b'), isTrue);
      expect(o.drain(), ['a', 'b']);
      expect(o.isEmpty, isTrue);
    });

    test('dropOldest evicts the head when full', () {
      final dropped = <String>[];
      final o = Outbox<String>(
          capacity: 2, policy: OverflowPolicy.dropOldest, onDrop: dropped.add);
      o
        ..add('a')
        ..add('b')
        ..add('c');
      expect(o.drain(), ['b', 'c']);
      expect(dropped, ['a']);
    });

    test('dropNewest rejects the incoming message when full', () {
      final dropped = <String>[];
      final o = Outbox<String>(
          capacity: 2, policy: OverflowPolicy.dropNewest, onDrop: dropped.add);
      o
        ..add('a')
        ..add('b')
        ..add('c');
      expect(o.drain(), ['a', 'b']);
      expect(dropped, ['c']);
    });

    test('dropOldest evicts even without an onDrop callback', () {
      final o = Outbox<String>(capacity: 1);
      o
        ..add('a')
        ..add('b');
      expect(o.drain(), ['b']);
    });

    test('capacity 0 buffers nothing', () {
      final dropped = <String>[];
      final o = Outbox<String>(capacity: 0, onDrop: dropped.add);
      o.add('a');
      expect(o.drain(), isEmpty);
      expect(dropped, ['a']);
    });

    test('negative capacity is rejected', () {
      expect(() => Outbox<String>(capacity: -1), throwsArgumentError);
    });
  });
}
