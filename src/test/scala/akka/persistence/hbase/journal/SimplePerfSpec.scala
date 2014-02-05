package akka.persistence.hbase.journal

import akka.persistence._
import akka.actor.{ActorSystem, Props}
import akka.testkit.{TestProbe, TestKit}
import org.scalatest.{DoNotDiscover, BeforeAndAfterAll, Matchers, FlatSpecLike}
import com.google.common.base.Stopwatch
import concurrent.duration._
import java.util.concurrent.TimeUnit
import akka.persistence.hbase.common.TestingEventProtocol._

object SimplePerfSpec {

  class Writer(Until: Int, override val processorId: String) extends Processor {

    def receive = {
      case Persistent(payload, sequenceNr) =>

      case Persistent(payload, Until) =>
        context.system.eventStream.publish (FinishedWrites(Until))
        sender ! FinishedWrites(Until)
    }
  }

}

@DoNotDiscover
class SimplePerfSpec extends TestKit(ActorSystem("test")) with FlatSpecLike
  with Matchers with BeforeAndAfterAll {

  import SimplePerfSpec._

  val config = system.settings.config.getConfig("hbase-journal")

  behavior of "HBaseJournal"

  override protected def beforeAll() {
    HBaseJournalInit.createTable(config)
  }

  val messagesNr = 80000

  it should s"write $messagesNr messages" in {
    // given
    val probe = TestProbe()
    system.eventStream.subscribe(probe.ref, classOf[FinishedWrites])

    val msg = Persistent("Hello!")

    val writer = system.actorOf(Props(classOf[Writer], messagesNr, "w-1"))

    // when
    val stopwatch = (new Stopwatch).start()

    var i = 1
    while (i <= messagesNr) {
      writer ! msg
      i = i + 1
    }

    // then
    probe.expectMsg(max = 2.minute, FinishedWrites(1))
    (messagesNr / 200 - 1).times { probe.expectMsg(max = 2.minute, FinishedWrites(200)); }
    probe.expectMsg(max = 2.minute, FinishedWrites(199))
    stopwatch.stop()
    system.eventStream.unsubscribe(probe.ref)

    info(s"Sending/persisting $messagesNr messages took: $stopwatch time")
    info(s"This is ${messagesNr / stopwatch.elapsedTime(TimeUnit.SECONDS)} m/s")

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
