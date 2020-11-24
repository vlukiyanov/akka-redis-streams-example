package api

import akka.actor.ActorSystem

import scala.language.postfixOps
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import io.lettuce.core.{RedisClient, StreamMessage, XReadArgs}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.wordspec.AnyWordSpecLike

import scala.collection.JavaConverters._
import scala.concurrent.duration.DurationInt

class RedisStreamsAckSinkTest
    extends TestKit(ActorSystem("TestingAkkaStreams"))
    with AnyWordSpecLike
    with BeforeAndAfterAll
    with Eventually {

  implicit val materializer = ActorMaterializer()

  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  "A RedisStreamsSource" must {
    "must be setup to accept all messages sent" in {
      val client: RedisClient = RedisClient.create("redis://localhost")
      val commands = client.connect.sync()
      val reactiveCommands = client.connect.reactive()
      commands.xtrim("testStream", 0)

      (1 to 100).foreach { _ =>
        commands.xadd("testStream", Map("a" -> "b").asJava)
      }

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

      val source = RedisStreamsSource.create(reactiveCommands,
                                             "testStream",
                                             "testGroup",
                                             "testConsumer")

      val f =
        source.toMat(TestSink.probe[StreamMessage[String, String]])(Keep.right)
      val probe = f.run()

      probe.request(100)
      probe.expectNextN(100)
      probe.request(1)
      probe.expectNoMessage(1.second)

      eventually {
        val p = commands.xpending("testStream", "testGroup")
        assert(p.getCount == 100)
      }

    }

  }

}
