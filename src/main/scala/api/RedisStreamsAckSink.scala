package api

import akka.NotUsed
import akka.stream.scaladsl.{Flow, Sink}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler}
import akka.stream.{Attributes, Inlet, SinkShape}
import org.redisson.Redisson
import org.redisson.api.{RStream, StreamMessageId}
import org.redisson.config.Config

import scala.concurrent.duration.{DurationInt, FiniteDuration}

object RedisStreamsAckSink {
  def createGrouped(stream: String, group: String, config: Option[Config] = None, batchSize: Int = 100, interval: FiniteDuration = 100.milliseconds): Sink[StreamMessageId, NotUsed] =
    Flow[StreamMessageId]
      .groupedWithin(batchSize, interval)
      .to(Sink.fromGraph(new RedisStreamsAckSink(stream, group, config)))

  def createBatch(stream: String, group: String, config: Option[Config] = None): Sink[Seq[StreamMessageId], NotUsed] =
    Sink.fromGraph(new RedisStreamsAckSink(stream, group, config))

  def create(stream: String, group: String, config: Option[Config] = None): Sink[StreamMessageId, NotUsed] =
    Flow[StreamMessageId].map(Seq(_)).to(new RedisStreamsAckSink(stream, group, config))
}

class RedisStreamsAckSink(stream: String, group: String, config: Option[Config] = None) extends GraphStage[SinkShape[Seq[StreamMessageId]]] {
  val in: Inlet[Seq[StreamMessageId]] = Inlet("RedisStreamsSink")
  override val shape: SinkShape[Seq[StreamMessageId]] = SinkShape(in)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      private lazy val redisson = config match {
        case Some(config) => Redisson.create(config)
        case None => Redisson.create
      }

      private lazy val s: RStream[String, String] = redisson.getStream(stream)

      override def preStart(): Unit = pull(in)

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          s.ack(group, grab(in): _*)
          pull(in)
        }

        override def onUpstreamFailure(ex: scala.Throwable): Unit = {
          redisson.shutdown()
          super.onUpstreamFailure(ex)
        }

        override def onUpstreamFinish(): Unit = {
          redisson.shutdown()
          super.onUpstreamFinish()
        }
      })

    }
}