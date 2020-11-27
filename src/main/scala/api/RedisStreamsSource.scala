package api

import akka.actor.Cancellable
import akka.Done
import akka.stream.OverflowStrategy
import akka.stream.scaladsl.Source
import io.lettuce.core.api.async.RedisAsyncCommands
import io.lettuce.core.{Consumer, StreamMessage, XReadArgs}

import scala.language.postfixOps
import scala.concurrent.duration.DurationInt
import scala.jdk.FutureConverters._
import scala.collection.JavaConverters._

/*
  Create a Source from a Redis stream, this uses XREADGROUP method for a given
  consumer group and consumer name.
 */
object RedisStreamsSource {
  def create(
      redis: RedisAsyncCommands[String, String],
      stream: String,
      group: String,
      consumer: String): Source[StreamMessage[String, String], Cancellable] = {
    Source
      .tick(0 millisecond, 100 millisecond, Done)
      .buffer(10, OverflowStrategy.dropTail)
      .mapAsync(8) { _ =>
        redis
          .xreadgroup(
            Consumer.from(group, consumer),
            XReadArgs.StreamOffset.lastConsumed(stream)
          )
          .asScala
      }
      .mapConcat(_.asScala.toList)
  }
}
