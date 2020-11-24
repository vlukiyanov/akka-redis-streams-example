package api

import akka.NotUsed
import akka.stream.scaladsl.Source._
import akka.stream.scaladsl.Flow
import io.lettuce.core.api.reactive.RedisReactiveCommands

import scala.collection.JavaConverters._

/*
  Create a Flow from a Redis stream, this uses XADD method and returns the
  string.
 */
object RedisStreamsFlow {
  def create(redis: RedisReactiveCommands[String, String],
             stream: String): Flow[Map[String, String], String, NotUsed] = {
    Flow
      .apply[Map[String, String]]
      .flatMapConcat(
        elem =>
          fromPublisher(
            redis
              .xadd(stream, elem.asJava)
        )
      )
  }
}
