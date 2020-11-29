package example

import akka.NotUsed
import akka.actor.ActorSystem
import akka.event.Logging
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.ActorMaterializer
import api.RedisStreamsFlow
import io.lettuce.core.RedisClient

import scala.concurrent.duration.DurationInt

object ProducerExample extends App {
  val client: RedisClient = RedisClient.create("redis://localhost")
  val asyncCommands = client.connect.async()

  implicit val system = ActorSystem("ProducerExample")
  implicit val materializer = ActorMaterializer()

  val redisStreamsFlow = RedisStreamsFlow.create(asyncCommands, "testStream")
  val messageSource = Source.repeat(Map("key" -> "test"))

  system.eventStream.setLogLevel(Logging.ErrorLevel)

  // start producing message to Redis; the rate measurement is from https://stackoverflow.com/a/49279641
  messageSource
    .via(redisStreamsFlow)
    .conflateWithSeed(_ => 0) { case (acc, _) => acc + 1 }
    .zip(Source.tick(1.second, 1.second, NotUsed))
    .map(_._1)
    .toMat(Sink.foreach(i => println(s"$i elements/second producer")))(
      Keep.right)
    .run()

}
