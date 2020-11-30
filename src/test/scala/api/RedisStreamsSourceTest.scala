package api

import akka.actor.ActorSystem

import scala.language.postfixOps
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import io.lettuce.core.{RedisClient, StreamMessage, XGroupCreateArgs, XReadArgs}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.wordspec.AnyWordSpecLike

import scala.collection.JavaConverters._
import scala.concurrent.duration.DurationInt

// These test are slightly rudimentary, mostly used when writing the code to test assumptions - require running Redis

class RedisStreamsSourceTest
  extends TestKit(ActorSystem("TestingAkkaStreams"))
    with AnyWordSpecLike
    with BeforeAndAfterAll
    with Eventually {

  implicit val materializer = ActorMaterializer()

  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  "A RedisStreamsSource" must {
    "must be setup to accept all messages sent" in {
      val client: RedisClient = RedisClient.create(scala.util.Properties.envOrElse("REDIS_URL", "redis://localhost"))
      val commands = client.connect.sync()
      val asyncCommands = client.connect.async()
      commands.xtrim("testStreamRedisStreamsSource", 0)

      (1 to 100).foreach { _ =>
        commands.xadd("testStreamRedisStreamsSource", Map("a" -> "b").asJava)
      }

      try {
        commands.xgroupDestroy("testStreamRedisStreamsSource", "testGroupSource")
      } catch {
        case _: Throwable => println("Group doesn't exist.")
      }

      try {
        commands.xgroupCreate(XReadArgs.StreamOffset.from("testStreamRedisStreamsSource", "0-0"),
          "testGroupSource", XGroupCreateArgs.Builder.mkstream())
      } catch {
        case _: Throwable => println("Group already exists.")
      }

      val source = RedisStreamsSource.create(asyncCommands,
        "testStreamRedisStreamsSource",
        "testGroupSource",
        "testConsumer")

      val f =
        source.toMat(TestSink.probe[StreamMessage[String, String]])(Keep.right)
      val probe = f.run()

      probe.request(100)
      probe.expectNextN(100)
      probe.request(1)
      probe.expectNoMessage(1.second)

      eventually {
        val p = commands.xpending("testStreamRedisStreamsSource", "testGroupSource")
        assert(p.getCount == 100)
      }

    }

  }

}
