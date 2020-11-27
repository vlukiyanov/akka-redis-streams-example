package api

import akka.NotUsed
import akka.stream.scaladsl.{Sink, Flow}
import io.lettuce.core.api.async.RedisAsyncCommands
import scala.jdk.FutureConverters._

/*
  Create a Sink from a Redis stream, this uses XACK method to acknowledge a particular
  message id.
 */
object RedisStreamsAckSink {
  def create(redis: RedisAsyncCommands[String, String],
             group: String,
             stream: String): Sink[String, NotUsed] = {
    Flow
      .apply[String]
      .mapAsync(4) { messageId =>
        redis.xack(stream, group, messageId).asScala
      }
      .to(Sink.ignore)
  }
}
