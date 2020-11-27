package api

import akka.actor.ActorSystem

import scala.language.postfixOps
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import io.lettuce.core.{RedisClient, XReadArgs}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.wordspec.AnyWordSpecLike

import scala.collection.JavaConverters._

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
      val asyncCommands = client.connect.async()
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

      val source = RedisStreamsSource.create(asyncCommands,
                                             "testStream",
                                             "testGroup",
                                             "testConsumer")

      val sink =
        RedisStreamsAckSink.create(asyncCommands, "testStream", "testGroup")

      source.map(_.getId).to(sink)

      eventually {
        val p = commands.xpending("testStream", "testGroup")
        assert(p.getCount == 0)
      }

    }

  }

}
