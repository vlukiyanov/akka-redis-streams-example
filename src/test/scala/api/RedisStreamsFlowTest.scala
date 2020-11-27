package api

import akka.actor.ActorSystem

import scala.language.postfixOps
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.testkit.TestKit
import io.lettuce.core.{RedisClient, StreamMessage, XReadArgs}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.wordspec.AnyWordSpecLike

import scala.collection.JavaConverters._
import scala.concurrent.duration.DurationInt

class RedisStreamsFlowTest
    extends TestKit(ActorSystem("TestingAkkaStreams"))
    with AnyWordSpecLike
    with BeforeAndAfterAll
    with Eventually {

  implicit val materializer = ActorMaterializer()

  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  "A RedisStreamsFlow" must {
    "must be setup to accept all messages sent" in {
      val client: RedisClient = RedisClient.create("redis://localhost")
      val commands = client.connect.sync()
      val asyncCommands = client.connect.async()
      commands.xtrim("testStream", 0)

      val flow = RedisStreamsFlow.create(asyncCommands, "testStream")

      val testSource = TestSource.probe[Map[String, String]]
      val testSink = TestSink.probe[String]

      val f = testSource.viaMat(flow)(Keep.left).toMat(testSink)(Keep.both)
      val (testSourceProbe, testSinkProbe) = f.run()

      testSourceProbe.sendNext(Map("a" -> "b", "c" -> "d"))
      testSinkProbe.request(1)
      testSinkProbe.expectNext()

      testSinkProbe.request(1)
      testSinkProbe.expectNoMessage(1.seconds)

      testSourceProbe.sendNext(Map("a" -> "b", "c" -> "d"))
      testSinkProbe.expectNext()

      testSinkProbe.cancel()
      testSinkProbe.expectNoMessage(1.seconds)

      eventually {
        val p = commands
          .xread(XReadArgs.StreamOffset.from("testStream", "0-0"))
          .asScala
        assert(p.length == 2)
      }

    }

  }

}
