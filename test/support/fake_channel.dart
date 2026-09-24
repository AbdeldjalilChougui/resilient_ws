import 'dart:async';

import 'package:stream_channel/stream_channel.dart';
import 'package:web_socket_channel/web_socket_channel.dart';

/// An in-memory [WebSocketChannel] whose handshake and lifetime the test
/// controls, so reconnect logic can run under fake time.
class FakeChannel with StreamChannelMixin<dynamic> implements WebSocketChannel {
  FakeChannel(this.url, this.headers);

  final Uri url;
  final Map<String, String> headers;
  final Completer<void> _ready = Completer<void>();
  final StreamController<dynamic> _incoming = StreamController<dynamic>();
  late final FakeSink _sink = FakeSink(this);

  /// Frames the client sent.
  final List<Object?> sent = [];

  bool clientClosed = false;
  int? clientCloseCode;

  @override
  int? closeCode;
  @override
  String? closeReason;
  @override
  String? get protocol => null;

  @override
  Future<void> get ready => _ready.future;

  @override
  Stream<dynamic> get stream => _incoming.stream;

  @override
  WebSocketSink get sink => _sink;

  /// Completes the handshake.
  void accept() => _ready.complete();

  /// Fails the handshake.
  void reject([Object error = 'refused']) {
    _ready.completeError(error);
    _incoming.close();
  }

  /// Delivers a frame from the "server".
  void push(Object? frame) => _incoming.add(frame);

  /// Closes from the "server" side.
  void serverClose([int? code, String? reason]) {
    closeCode = code;
    closeReason = reason;
    _incoming.close();
  }
}

class FakeSink implements WebSocketSink {
  FakeSink(this.channel);
  final FakeChannel channel;

  @override
  void add(Object? data) {
    if (channel.clientClosed) throw StateError('sink closed');
    channel.sent.add(data);
  }

  @override
  void addError(Object error, [StackTrace? stackTrace]) {}

  @override
  Future<void> addStream(Stream<Object?> stream) => stream.forEach(add);

  @override
  Future<void> close([int? closeCode, String? closeReason]) async {
    channel.clientClosed = true;
    channel.clientCloseCode = closeCode;
    if (!channel._incoming.isClosed) {
      channel.closeCode = closeCode;
      unawaited(channel._incoming.close());
    }
  }

  @override
  Future<void> get done => Future<void>.value();
}
