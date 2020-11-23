package api

import akka.actor.Cancellable
import akka.{Done, NotUsed}
import akka.stream.scaladsl.Source
import io.lettuce.core.{Consumer, StreamMessage, XReadArgs}
import io.lettuce.core.api.reactive.RedisReactiveCommands

import scala.language.postfixOps
import scala.concurrent.duration.DurationInt

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
