package api

import akka.actor.Cancellable
import akka.Done
import akka.stream.scaladsl.Source
import io.lettuce.core.{Consumer, StreamMessage, XReadArgs}
import io.lettuce.core.api.reactive.RedisReactiveCommands

import scala.language.postfixOps
import scala.concurrent.duration.DurationInt

/*
  Create a Source from a Redis stream, this uses XREADGROUP method for a given
  consumer group and consumer name.
 */
object RedisStreamsSource {
  def create(
      redis: RedisReactiveCommands[String, String],
      stream: String,
      group: String,
      consumer: String): Source[StreamMessage[String, String], Cancellable] = {
    Source
      .tick(0 millisecond, 100 millisecond, Done)
      .flatMapConcat(
        _ =>
          Source.fromPublisher(
            redis
              .xreadgroup(
                Consumer.from(group, consumer),
                XReadArgs.StreamOffset.lastConsumed(stream)
              )
        ))
  }
}
