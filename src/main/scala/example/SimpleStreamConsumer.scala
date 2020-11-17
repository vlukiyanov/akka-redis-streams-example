package example

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, GraphDSL, RunnableGraph, Sink, Source}
import akka.stream.{ActorMaterializer, ClosedShape}
import api.{RedisStreamsAckSink, RedisStreamsFlow, RedisStreamsSource}
import org.redisson.Redisson
import org.redisson.api.{RStream, StreamMessageId}

import scala.concurrent.Future

object SimpleStreamConsumer extends App {

  implicit val system = ActorSystem("FirstPrinciples")
  implicit val materializer = ActorMaterializer()

  val redisson = Redisson.create

  val s: RStream[String, String] = redisson.getStream("test")

  val f = Source.fromGraph(new RedisStreamsSource("test", "test", "test"))
  val g = Flow.fromGraph(new RedisStreamsFlow("test"))
  val a = Sink.fromGraph(new RedisStreamsAckSink("test", "test"))

  val leftFlow: Sink[(StreamMessageId, String, String), NotUsed] = Flow
    .fromFunction(x => x._1)
    .to(a)

  val flow = RunnableGraph.fromGraph(GraphDSL.create(f, leftSink, rightSink)
  ((_, _, _)) { implicit b =>
    (s, l, r) =>


      sourceInput ~> pw.in
      pw.out0 ~> leftSink
      pw.out1 ~> rightSink

      ClosedShape
  })

  // TODO handle consumer group creation
  // TODO handle serialisation

  for {
    v <- 1 to 1212
  } {
    s.addAsync("test", s"$v").asScala
  }

  Future {
    Thread.sleep(1000)
  }.onComplete(_ => {
    for {
      v <- 1 to 9
    } {
      s.addAsync("test", s"$v").asScala
    }
  })

  f
    .mapAsync(1) {
      item =>
        Future {
          item._1
        }
    }
    .map(item => {
      item
    })
    .groupedWithin(100, 100.millisecond)
    .to(a)
    .run()

}
