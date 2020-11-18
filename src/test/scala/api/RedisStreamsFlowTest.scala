package api

import akka.actor.ActorSystem

import scala.language.postfixOps
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Keep
import akka.stream.testkit.{TestPublisher, TestSubscriber}
import akka.stream.testkit.scaladsl.{TestSink, TestSource}
import akka.testkit.TestKit
import org.redisson.Redisson
import org.redisson.api.{RStream, StreamMessageId}
import org.redisson.client.codec.StringCodec
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration.DurationInt

class RedisStreamsFlowTest extends TestKit(ActorSystem("TestingAkkaStreams"))
  with AnyWordSpecLike
  with BeforeAndAfterAll
  with Eventually {

  implicit val materializer = ActorMaterializer()

  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  "A RedisStreamsFlow" must {
    "must be setup to add all message sent" in {
      val redisson = Redisson.create
      val s: RStream[String, String] = redisson.getStream("testStream", new StringCodec("UTF-8"))
      s.trim(0)
      try {
        s.removeGroup("testGroup")
        s.createGroup("testGroup")
      } catch {
        case _: Throwable => println("Group already exists.")
      }

      val flow = RedisStreamsFlow.create("testStream")

      val testSource = TestSource.probe[Map[String, String]]
      val testSink = TestSink.probe[StreamMessageId]

      val f = testSource.viaMat(flow)(Keep.left).toMat(testSink)(Keep.both)
      val (testSourceProbe, testSinkProbe): (TestPublisher.Probe[Map[String, String]], TestSubscriber.Probe[StreamMessageId]) = f.run()

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
        val p = s.size()
        assert(p == 2)
      }

    }

  }

}