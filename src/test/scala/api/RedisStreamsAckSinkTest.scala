package api

import akka.actor.ActorSystem

import scala.language.postfixOps
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import io.lettuce.core.{Consumer, RedisClient, XGroupCreateArgs, XReadArgs}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpecLike

import scala.collection.JavaConverters._

// These test are slightly rudimentary, mostly used when writing the code to test assumptions - require running Redis

class RedisStreamsAckSinkTest
    extends TestKit(ActorSystem("TestingAkkaStreams"))
    with AnyWordSpecLike
    with BeforeAndAfterAll
    with Eventually {

  implicit override val patienceConfig: PatienceConfig = PatienceConfig(
    timeout = scaled(Span(10, Seconds)),
    interval = scaled(Span(500, Millis)))

  implicit val materializer = ActorMaterializer()

  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  "A RedisStreamsAckSink" must {
    "must be setup to accept all messages sent" in {
      val client: RedisClient = RedisClient.create(
        scala.util.Properties.envOrElse("REDIS_URL", "redis://localhost"))
      val clientSink: RedisClient = RedisClient.create(
        scala.util.Properties.envOrElse("REDIS_URL", "redis://localhost"))

      val commands = client.connect.sync()
      val asyncCommands = client.connect.async()
      val asyncCommandsSink = clientSink.connect.async()
      commands.xtrim("testStreamRedisStreamsAckSink", 0)

      try {
        commands.xgroupDestroy("testStreamRedisStreamsAckSink",
                               "testGroupAckSink")
      } catch {
        case _: Throwable => println("Group doesn't exist.")
      }

      try {
        commands.xgroupCreate(
          XReadArgs.StreamOffset.from("testStreamRedisStreamsAckSink", "0-0"),
          "testGroupAckSink",
          XGroupCreateArgs.Builder.mkstream())
      } catch {
        case _: Throwable => println("Group already exists.")
      }

      eventually {
        commands.xinfoGroups("testStreamRedisStreamsAckSink").size() == 1
      }

      (1 to 10000).foreach { _ =>
        commands.xadd("testStreamRedisStreamsAckSink", Map("a" -> "b").asJava)
      }

      val source = RedisStreamsSource.create(asyncCommands,
                                             "testStreamRedisStreamsAckSink",
                                             "testGroupAckSink",
                                             "testConsumer")

      val sink =
        RedisStreamsAckSink.create(asyncCommandsSink,
                                   "testGroupAckSink",
                                   "testStreamRedisStreamsAckSink")

      source
        .map(_.getId)
        .to(sink)
        .run()

      eventually {
        val p =
          commands.xpending("testStreamRedisStreamsAckSink", "testGroupAckSink")
        assert(p.getCount > 0)
      }

      eventually {
        val p =
          commands.xpending("testStreamRedisStreamsAckSink", "testGroupAckSink")
        assert(p.getCount == 0)
      }

    }

  }

}
