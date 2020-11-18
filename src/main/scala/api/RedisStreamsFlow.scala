package api

import akka.NotUsed
import akka.stream.scaladsl.Flow
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import org.redisson.Redisson
import org.redisson.api.{RStream, StreamMessageId}
import org.redisson.client.codec.StringCodec
import org.redisson.config.Config

import scala.jdk.CollectionConverters._

object RedisStreamsFlow {
  def create(stream: String, config: Option[Config] = None): Flow[Map[String, String], StreamMessageId, NotUsed] = {
    Flow.fromGraph(new RedisStreamsFlow(stream, config))
  }
}

class RedisStreamsFlow(stream: String, config: Option[Config] = None) extends GraphStage[FlowShape[Map[String, String], StreamMessageId]] {
  val in: Inlet[Map[String, String]] = Inlet("RedisStreamsFlowInlet")
  val out: Outlet[StreamMessageId] = Outlet("RedisStreamsFlowOutlet")
  override val shape: FlowShape[Map[String, String], StreamMessageId] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      private lazy val redisson = config match {
        case Some(config) => Redisson.create(config)
        case None => Redisson.create
      }
      // TODO add possibility to handle other codecs, in particular for serializable data like JSON
      private lazy val s: RStream[String, String] = redisson.getStream(stream, new StringCodec("UTF-8"))

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          push(out, s.addAll(grab(in).asJava))
        }

        override def onUpstreamFinish(): Unit = {
          redisson.shutdown()
          super.onUpstreamFinish()
        }

        override def onUpstreamFailure(ex: scala.Throwable): Unit = {
          redisson.shutdown()
          super.onUpstreamFailure(ex)
        }
      })

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          pull(in)
        }

        override def onDownstreamFinish(cause: scala.Throwable): Unit = {
          redisson.shutdown()
          super.onDownstreamFinish(cause)
        }
      })
    }
}