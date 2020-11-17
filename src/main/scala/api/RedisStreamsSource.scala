package api

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

object RedisStreamsSource {
  sealed case class RedisMessage(id: StreamMessageId, key: String, value: String)

  def create(stream: String, group: String, consumer: String, config: Option[Config] = None): Source[RedisMessage, NotUsed] =
    Source.fromGraph(new RedisStreamsSource(stream, group, consumer, config))
}

class RedisStreamsSource(stream: String, group: String, consumer: String, config: Option[Config] = None) extends GraphStage[SourceShape[RedisMessage]] {
  val out: Outlet[RedisMessage] = Outlet("api.RedisStreamsSource")
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

          val elements = for {
            item <- s.readGroup(group, consumer).entrySet.asScala
            key = item.getKey
            value = item.getValue.entrySet.asScala.head
          } yield RedisMessage(key, value.getKey, value.getValue)

          if(elements.isEmpty) {
            Thread.sleep(150)
            this.onPull()
          } else {
            emitMultiple(out, elements.toList)
          }
        }
      })
    }
  }
}