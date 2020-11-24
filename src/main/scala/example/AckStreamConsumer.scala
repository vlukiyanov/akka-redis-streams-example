package example

import akka.NotUsed
import akka.actor.{ActorSystem, Cancellable}
import akka.event.Logging
import akka.stream.scaladsl.{
  Broadcast,
  Flow,
  GraphDSL,
  Keep,
  RunnableGraph,
  Sink,
  Source
}
import akka.stream.{ActorMaterializer, ClosedShape}
import api.{RedisStreamsAckSink, RedisStreamsFlow, RedisStreamsSource}
import io.lettuce.core.{RedisClient, XReadArgs}
import org.redisson.Redisson
import org.redisson.api.{RStream, StreamMessageId}
import org.redisson.client.codec.StringCodec

import scala.concurrent.duration.DurationInt

object AckStreamConsumer extends App {
  val client: RedisClient = RedisClient.create("redis://localhost")
  val commands = client.connect.sync()
  val reactiveCommands = client.connect.reactive()

  commands.xtrim("testStream", 0)
  try {
    commands.xgroupDestroy("testStream", "testGroup")
  } catch {
    case _: Throwable => println("Group doesn't exist.")
  }

  try {
    commands.xgroupCreate(XReadArgs.StreamOffset.from("testStream", "0-0"),
                          "testGroup")
  } catch {
    case _: Throwable => println("Group already exists.")
  }

  implicit val system = ActorSystem("FirstPrinciples")
  implicit val materializer = ActorMaterializer()

  val redisStreamsFlow = RedisStreamsFlow.create(reactiveCommands, "testStream")
  val messageSource = Source(1 to 1000000 map { i =>
    Map(s"$i" -> "test")
  })

  system.eventStream.setLogLevel(Logging.ErrorLevel)

  val ackSink: Sink[String, NotUsed] =
    RedisStreamsAckSink.create(reactiveCommands, "testStream", "testGroup")

  // rate measurement is from https://stackoverflow.com/a/49279641
  val statsSink = Flow
    .apply[String]
    .conflateWithSeed(_ => 0) { case (acc, _) => acc + 1 }
    .zip(Source.tick(1.second, 1.second, NotUsed))
    .map(_._1)
    .toMat(Sink.foreach(i => println(s"$i elements/second")))(Keep.right)

  val graph = RunnableGraph.fromGraph(GraphDSL.create() { implicit builder =>
    import GraphDSL.Implicits._
    val bcast = builder.add(Broadcast[String](2))

    messageSource.via(redisStreamsFlow) ~> bcast.in
    bcast.out(0) ~> Flow[String].async ~> ackSink
    bcast.out(1) ~> Flow[String].async ~> statsSink

    ClosedShape
  })

  graph.run()

}
