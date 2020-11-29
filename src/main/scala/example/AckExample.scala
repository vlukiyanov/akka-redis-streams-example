package example

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.ActorMaterializer
import api.{RedisStreamsAckSink, RedisStreamsFlow, RedisStreamsSource}
import io.lettuce.core.{RedisClient, XReadArgs}

import scala.concurrent.duration.DurationInt

object AckExample extends App {
  val clientProducer: RedisClient = RedisClient.create("redis://localhost")
  val clientConsumer: RedisClient = RedisClient.create("redis://localhost")
  val commands = clientProducer.connect.sync()
  val asyncCommandsProducer = clientProducer.connect.async()
  val asyncCommandsConsumer = clientConsumer.connect.async()


  implicit val system = ActorSystem("ConsumerExample")
  implicit val materializer = ActorMaterializer()

  commands.xtrim("testStream", 0)

  val redisStreamsFlow = RedisStreamsFlow.create(asyncCommandsProducer, "testStream")
  val messageSource = Source(1 to 1_000_000).map(_ => Map("key" -> "test"))

  // the rate measurement is from https://stackoverflow.com/a/49279641
  messageSource
    .via(redisStreamsFlow)
    .conflateWithSeed(_ => 0) { case (acc, _) => acc + 1 }
    .zip(Source.tick(1.second, 1.second, NotUsed))
    .map(_._1)
    .toMat(Sink.foreach(i => println(s"$i elements/second producer")))(Keep.right)
    .run()

  try {
    commands.xgroupDestroy("testStream", "testGroup")
    println("Deleted group")
  } catch {
    case _: Throwable => println("Group doesn't exist.")
  }

  try {
    commands.xgroupCreate(XReadArgs.StreamOffset.from("testStream", "0-0"),
      "testGroup")
    println("Created group")
  } catch {
    case _: Throwable => println("Group already exists.")
  }

  val redisStreamsSource = RedisStreamsSource.create(asyncCommandsConsumer,
    "testStream",
    "testGroup",
    "testConsumer")

  val redisStreamAckSink = RedisStreamsAckSink.create(asyncCommandsConsumer, "testGroup", "testStream")

  // do no ack the messages; the rate measurement is from https://stackoverflow.com/a/49279641
  redisStreamsSource
    .map(_.getId)
    .to(redisStreamAckSink)
    .run()

}
