package example

import akka.NotUsed
import akka.actor.ActorSystem
import akka.event.Logging
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.ActorMaterializer
import api.{RedisStreamsFlow, RedisStreamsSource}
import io.lettuce.core.{RedisClient, XReadArgs}

import scala.concurrent.duration.DurationInt

object SimpleStreamConsumer extends App {
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

  // start producing message to Redis
  messageSource
    .via(redisStreamsFlow)
    .to(Sink.ignore)
    .run()

  val redisStreamsSource = RedisStreamsSource.create(reactiveCommands,
                                                     "testStream",
                                                     "testGroup",
                                                     "testConsumer")

  // do no ack the messages; the rate measurement is from https://stackoverflow.com/a/49279641
  redisStreamsSource.async
    .conflateWithSeed(_ => 0) { case (acc, _) => acc + 1 }
    .zip(Source.tick(1.second, 1.second, NotUsed))
    .map(_._1)
    .toMat(Sink.foreach(i => println(s"$i elements/second")))(Keep.right)
    .run()

}
