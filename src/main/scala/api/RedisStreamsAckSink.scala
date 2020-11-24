package api

import akka.NotUsed
import akka.stream.scaladsl.Source._
import akka.stream.scaladsl.{Flow, Sink}
import io.lettuce.core.api.reactive.RedisReactiveCommands

/*
  Create a Sink from a Redis stream, this uses XACK method to acknowledge a particular
  message id.
 */
object RedisStreamsAckSink {
  def create(redis: RedisReactiveCommands[String, String],
             group: String,
             stream: String): Sink[String, NotUsed] = {
    Flow
      .apply[String]
      .flatMapConcat(
        messageId =>
          fromPublisher(
            redis
              .xack(stream, group, messageId)
        )
      )
      .to(Sink.ignore)
  }
}
