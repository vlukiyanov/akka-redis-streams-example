# akka-redis-streams-example
An simple proof of concept example using Redis streams in Akka streams which provides 3 components:

* `RedisStreamsAckSink` provides methods to create `Sink[StreamMessageId, NotUsed]` or `Sink[Seq[StreamMessageId], NotUsed]` for acknowledging messages in single or batches.
* `RedisStreamsFlow` provides methods to create `Flow[Map[String, String], StreamMessageId, NotUsed]` to add messages to a stream.
* `RedisStreamsSource` provides methods to consume messages from a stream as `Source[RedisMessage, NotUsed]`, a consumer group and a consumer (the consumer group should be created beforehand).

This is missing handling of unacknowledged messages using some combination of `XCLAIM` and `XPENDING`.

This uses https://github.com/redisson/redisson underneath.
