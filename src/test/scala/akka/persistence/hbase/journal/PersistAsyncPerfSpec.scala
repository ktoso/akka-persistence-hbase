package akka.persistence.hbase.journal

import java.util.Date
import java.util.concurrent.TimeUnit

import akka.actor.{ActorLogging, ActorSystem, Props}
import akka.persistence._
import akka.persistence.hbase.common.TestingEventProtocol._
import akka.testkit.{TestKit, TestProbe}
import com.google.common.base.Stopwatch
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.duration._

object PersistAsyncPerfSpec {

  class Writer(untilSeqNr: Int, override val persistenceId: String) extends PersistentActor
    with ActorLogging {

    def receiveCommand = {
      case payload if lastSequenceNr != untilSeqNr =>
        persistAsync(payload) { p => log.debug(s"persisted: {} @ {}", p, lastSequenceNr) }

      case payload =>
        persistAsync(payload) { p =>
          sender ! FinishedWrites(lastSequenceNr)

          log.info("Deleting messages in {}, until {}", persistenceId, lastSequenceNr)
          deleteMessages(toSequenceNr = lastSequenceNr)
        }
    }

    override def receiveRecover: Receive = {
      case m => println("recover: " + m)
    }
  }

}

//@DoNotDiscover
class PersistAsyncPerfSpec extends TestKit(ActorSystem("test")) with FlatSpecLike
  with Matchers with BeforeAndAfterAll {

  import akka.persistence.hbase.journal.PersistAsyncPerfSpec._

  val config = system.settings.config

  behavior of "HBaseJournal"

  override protected def beforeAll() {
    HBaseJournalInit.createTable(config)
  }

  val messagesNr = 1000

  val messages = (1 to messagesNr) map { i => s"hello-$i-(${new Date})" }

  it should s"write $messagesNr messages" in {
    // given
    val probe = TestProbe()
    system.eventStream.subscribe(probe.ref, classOf[FinishedWrites])

    val writer = system.actorOf(Props(classOf[Writer], messagesNr, "w-1"))

    // when
    val stopwatch = (new Stopwatch).start()

    messages foreach { writer ! _ }

    // then
    probe.expectMsg(max = 2.minute, FinishedWrites(1))
    (messagesNr / 200 - 1).times { probe.expectMsg(max = 2.minute, FinishedWrites(200)); }
    probe.expectMsg(max = 2.minute, FinishedWrites(199))
    stopwatch.stop()
    system.eventStream.unsubscribe(probe.ref)

    info(s"Sending/persisting $messagesNr messages took: $stopwatch time")
    info(s"This is ${messagesNr / stopwatch.elapsedTime(TimeUnit.SECONDS)} msg/s")

  }

  implicit class TimesInt(i: Int) {
    def times(block: => Unit) = {
      1 to i foreach { _ => block }
    }
  }

  def timed(block: => Unit): Stopwatch = {
    val stopwatch = (new Stopwatch).start()
    block
    stopwatch.stop()
  }
}
