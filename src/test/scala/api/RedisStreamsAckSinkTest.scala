package api

import akka.NotUsed
import akka.actor.ActorSystem

import scala.language.postfixOps
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink}
import akka.stream.testkit.scaladsl.TestSource
import akka.testkit.TestKit
import org.redisson.Redisson
import org.redisson.api.{RStream, StreamMessageId}
import org.redisson.client.codec.StringCodec
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.wordspec.AnyWordSpecLike

class RedisStreamsAckSinkTest extends TestKit(ActorSystem("TestingAkkaStreams"))
  with AnyWordSpecLike
  with BeforeAndAfterAll
  with Eventually {

  implicit val materializer = ActorMaterializer()

  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  "A RedisStreamsAckSink" must {
    "must be setup with createGrouped to accept StreamMessageId" in {
      val redisson = Redisson.create
      val s: RStream[String, String] = redisson.getStream("testStream", new StringCodec("UTF-8"))
      s.trim(0)
      try {
        s.removeGroup("testGroup")
        s.createGroup("testGroup")
      } catch {
        case _: Throwable => println("Group already exists.")
      }

      val redisStreamAck: Sink[StreamMessageId, NotUsed] = RedisStreamsAckSink.createGrouped("testStream", "testGroup")

      val l: List[StreamMessageId] = for {
        _ <- 1 to 10 toList
      } yield s.add("test", "test")
      s.readGroup("testGroup", "testConsumer")

      val f = TestSource.probe[StreamMessageId].to(redisStreamAck)
      val probe = f.run()

      for { _ <- 1 to 1000} {
        probe.sendNext(l.head)
      }

      probe.sendComplete()

      eventually {
        val p = s.getPendingInfo("testGroup")
        assert(p.getTotal == 9)
      }

    }

    "must be setup with create to accept StreamMessageId" in {
      val redisson = Redisson.create
      val s: RStream[String, String] = redisson.getStream("testStream", new StringCodec("UTF-8"))
      s.trim(0)
      try {
        s.removeGroup("testGroup")
        s.createGroup("testGroup")
      } catch {
        case _: Throwable => println("Group already exists.")
      }

      val redisStreamAck: Sink[StreamMessageId, NotUsed] = RedisStreamsAckSink.create("testStream", "testGroup")

      val l: List[StreamMessageId] = for {
        _ <- 1 to 10 toList
      } yield s.add("test", "test")
      s.readGroup("testGroup", "testConsumer")

      val f = TestSource.probe[StreamMessageId].to(redisStreamAck)
      val probe = f.run()

      probe.sendNext(l.head)
      probe.sendComplete()

      eventually {
        val p = s.getPendingInfo("testGroup")
        assert(p.getTotal == 9)
      }

    }

    "must be setup with createBatch to accept Seq[StreamMessageId]" in {
      val redisson = Redisson.create
      val s: RStream[String, String] = redisson.getStream("testStream", new StringCodec("UTF-8"))
      s.trim(0)
      try {
        s.removeGroup("testGroup")
        s.createGroup("testGroup")
      } catch {
        case _: Throwable => println("Group already exists.")
      }

      val redisStreamAck: Sink[Seq[StreamMessageId], NotUsed] = RedisStreamsAckSink.createBatch("testStream", "testGroup")

      val l: List[StreamMessageId] = for {
        _ <- 1 to 10 toList
      } yield s.add("test", "test")
      s.readGroup("testGroup", "testConsumer")

      val f = TestSource.probe[Seq[StreamMessageId]].to(redisStreamAck)
      val probe = f.run()

      probe.sendNext(l)
      probe.sendComplete()

      eventually {
        val p = s.getPendingInfo("testGroup")
        assert(p.getTotal == 0)
      }

    }
  }

}