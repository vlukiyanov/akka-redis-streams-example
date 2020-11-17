# akka-redis-streams-example
An simple proof of concept example of using Redis streams in Akka, mostly for learning about how Redis Streams works, provides 3 components:

* `RedisStreamsAckSink` provides methods to create `Sink[StreamMessageId, NotUsed]` or `Sink[Seq[StreamMessageId], NotUsed]` for acknowledging messages in single or batches.
* `RedisStreamsFlow` provides methods to create `FlowShape[Map[String, String], StreamMessageId]` to add messages to a stream.
* `RedisStreamsSource` provides methods to consume messages from a stream, a consumer group and a consumer (the consumer group should be created beforehand).

This uses https://github.com/redisson/redisson underneath.
