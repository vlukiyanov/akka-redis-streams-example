package example

import akka.NotUsed
import akka.actor.ActorSystem
import akka.event.Logging
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{ActorMaterializer, ClosedShape}
import api.{RedisStreamsFlow, RedisStreamsSource}
import org.redisson.Redisson
import org.redisson.api.RStream
import org.redisson.client.codec.StringCodec

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt


object SimpleStreamConsumer extends App {
  val redisson = Redisson.create
  val s: RStream[String, String] = redisson.getStream("testStream", new StringCodec("UTF-8"))
  s.trim(0)
  try {
    s.removeGroup("testGroup")
    s.createGroup("testGroup")
  } catch {
    case _: Throwable => println("Group already exists.")
  }

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

  val redisStreamsSource = RedisStreamsSource.create("testStream", "testGroup", "testConsumer")

  // do no ack the messages; the rate measurement is from https://stackoverflow.com/a/49279641
  redisStreamsSource
    .mapAsyncUnordered(24) {
      item => Future { item.key.toInt % 100000 }
    }
    .conflateWithSeed(_ => 0){ case (acc, _) => acc + 1 }
    .zip(Source.tick(1.second, 1.second, NotUsed))
    .map(_._1)
    .toMat(Sink.foreach(i => println(s"$i elements/second")))(Keep.right)
    .run()

}
