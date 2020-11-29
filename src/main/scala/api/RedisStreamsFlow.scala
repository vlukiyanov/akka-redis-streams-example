package api

import akka.NotUsed
import akka.stream.scaladsl.Flow
import io.lettuce.core.api.async.RedisAsyncCommands

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.jdk.FutureConverters._

/*
  Create a Flow from a Redis stream, this uses XADD method and returns the
  string.
 */
object RedisStreamsFlow {
  def create(redis: RedisAsyncCommands[String, String],
             stream: String): Flow[Map[String, String], String, NotUsed] = {
    redis.setAutoFlushCommands(false)
    Flow
      .apply[Map[String, String]]
      .groupedWithin(1000, 1.second)
      .mapAsync(4) { elems =>
        {
          val futures =
            elems
              .map(elem => redis.xadd(stream, elem.asJava).asScala)
          redis.flushCommands()
          Future.sequence(futures)
        }
      }
      .mapConcat(identity)
  }
}
