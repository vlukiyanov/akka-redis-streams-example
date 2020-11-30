package example

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.ActorMaterializer
import api.RedisStreamsSource
import io.lettuce.core.{RedisClient, XGroupCreateArgs, XReadArgs}

import scala.concurrent.duration.DurationInt

object ConsumerExample extends App {
  val client: RedisClient = RedisClient.create("redis://localhost")
  val commands = client.connect.sync()
  val asyncCommands = client.connect.async()

  try {
    commands.xgroupDestroy("testStream", "testGroup")
    println("Deleted group")
  } catch {
    case _: Throwable => println("Group doesn't exist.")
  }

  try {
    commands.xgroupCreate(XReadArgs.StreamOffset.from("testStream", "0-0"),
                          "testGroup",
                          XGroupCreateArgs.Builder.mkstream())
    println("Created group")
  } catch {
    case _: Throwable => println("Group already exists.")
  }

  implicit val system = ActorSystem("ConsumerExample")
  implicit val materializer = ActorMaterializer()

  val redisStreamsSource = RedisStreamsSource.create(asyncCommands,
                                                     "testStream",
                                                     "testGroup",
                                                     "testConsumer")

  // do no ack the messages; the rate measurement is from https://stackoverflow.com/a/49279641
  redisStreamsSource
    .conflateWithSeed(_ => 0) { case (acc, _) => acc + 1 }
    .zip(Source.tick(1.second, 1.second, NotUsed))
    .map(_._1)
    .toMat(Sink.foreach(i => println(s"$i elements/second consumer")))(
      Keep.right)
    .run()

}
