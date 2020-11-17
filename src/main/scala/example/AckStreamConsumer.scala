package example

import akka.NotUsed
import akka.actor.ActorSystem
import akka.event.Logging
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Keep, RunnableGraph, Sink, Source}
import akka.stream.{ActorMaterializer, ClosedShape}
import api.{RedisStreamsAckSink, RedisStreamsFlow, RedisStreamsSource}
import org.redisson.api.StreamMessageId

import scala.concurrent.duration.DurationInt


object AckStreamConsumer extends App {

  implicit val system = ActorSystem("FirstPrinciples")
  implicit val materializer = ActorMaterializer()

  val redisStreamsFlow = RedisStreamsFlow.create("testStream")
  val messageSource = Source(1 to 1000000 map { i => Map(s"$i" -> "test")})

  system.eventStream.setLogLevel(Logging.ErrorLevel)

  // start producing message to Redis
  messageSource
    .via(redisStreamsFlow)
    .to(Sink.ignore)
    .run()

  val redisStreamsSource = RedisStreamsSource.create("testStream", "testGroup", "testConsumer").map(_.id)

  val ackSink: Sink[StreamMessageId, NotUsed] = RedisStreamsAckSink.createGrouped("testStream", "testGroup")

  // rate measurement is from https://stackoverflow.com/a/49279641
  val statsSink = Flow.apply[StreamMessageId].conflateWithSeed(_ => 0){ case (acc, _) => acc + 1 }
    .zip(Source.tick(1.second, 1.second, NotUsed))
    .map(_._1)
    .toMat(Sink.foreach(i => println(s"$i elements/second")))(Keep.right)

  val graph = RunnableGraph.fromGraph(GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._
    val bcast = builder.add(Broadcast[StreamMessageId](2))

    redisStreamsSource ~> bcast.in
    // these run sync to measure stuff, add ~> Flow[StreamMessageId].async ~> or something to run the two outlets async
    bcast.out(0) ~>  ackSink
    bcast.out(1) ~> statsSink

    ClosedShape
  })

  graph.run()

}
