package api

import akka.actor.ActorSystem

import scala.language.postfixOps
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Keep
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import api.RedisStreamsSource.RedisMessage
import org.redisson.Redisson
import org.redisson.api.RStream
import org.redisson.client.codec.StringCodec
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration.DurationInt

class RedisStreamsSourceTest extends TestKit(ActorSystem("TestingAkkaStreams"))
  with AnyWordSpecLike
  with BeforeAndAfterAll
  with Eventually {

  implicit val materializer = ActorMaterializer()

  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  "A RedisStreamsSource" must {
    "must be setup to accept all messages sent" in {
      val redisson = Redisson.create
      val s: RStream[String, String] = redisson.getStream("testStream", new StringCodec("UTF-8"))
      s.trim(0)
      try {
        s.removeGroup("testGroup")
        s.createGroup("testGroup")
      } catch {
        case _: Throwable => println("Group already exists.")
      }

      val source = RedisStreamsSource.create("testStream", "testGroup", "testConsumer")

      val f = source.toMat(TestSink.probe[RedisMessage])(Keep.right)
      val probe = f.run()

      probe.request(100)
      probe.expectNextN(100)
      probe.request(1)
      probe.expectNoMessage(1.second)

      eventually {
        val p = s.getPendingInfo("testGroup")
        assert(p.getTotal == 100)
      }

    }

  }

}