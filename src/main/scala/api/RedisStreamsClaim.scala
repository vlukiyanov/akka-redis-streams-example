package api

import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.stream.scaladsl.Source
import akka.stream.stage.{GraphStage, GraphStageLogic, OutHandler}
import akka.stream.{Attributes, Outlet, SourceShape}
import api.RedisStreamsSource.RedisMessage
import org.redisson.Redisson
import org.redisson.api.{RStream, StreamMessageId}
import org.redisson.client.codec.StringCodec
import org.redisson.config.Config

import scala.annotation.tailrec
import scala.jdk.CollectionConverters._

object RedisStreamsClaim {
  def create(stream: String, group: String, consumer: String, timeOut: Long, config: Option[Config] = None): Source[RedisMessage, NotUsed] =
    Source.fromGraph(new RedisStreamsClaim(stream, group, consumer, timeOut, config))
}

class RedisStreamsClaim(stream: String, group: String, consumer: String, timeOut: Long, config: Option[Config] = None) extends GraphStage[SourceShape[RedisMessage]] {
  val out: Outlet[RedisMessage] = Outlet("api.RedisStreamsClaim")
  override val shape: SourceShape[RedisMessage] = SourceShape(out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new GraphStageLogic(shape) {
      private lazy val redisson = config match {
        case Some(config) => Redisson.create(config)
        case None => Redisson.create
      }
      // TODO add possibility to handle other codecs, in particular for serializable data like JSON
      private lazy val s: RStream[String, String] = redisson.getStream(stream, new StringCodec("UTF-8"))

      setHandler(out, new OutHandler {
        @tailrec
        override def onPull(): Unit = {

          val messages: List[StreamMessageId] = for {
            item <- s.listPending(group, consumer, StreamMessageId.MIN, StreamMessageId.MAX, 1000).asScala.toList
            if item.getIdleTime > timeOut
          } yield item.getId

          if(messages.isEmpty) {
            Thread.sleep(150)
            this.onPull()
          } else {
            val elements = for {
              item <- s.claim(group, consumer, timeOut, TimeUnit.MILLISECONDS, messages: _*).entrySet.asScala
              key = item.getKey
              value = item.getValue.entrySet.asScala.head
            } yield RedisMessage(key, value.getKey, value.getValue)
            emitMultiple(out, elements.toList)
          }
        }

        override def onDownstreamFinish(cause: scala.Throwable): Unit = {
          redisson.shutdown()
          super.onDownstreamFinish(cause)
        }
      })
    }
  }
}