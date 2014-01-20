package akka.persistence.journal.hbase

import akka.persistence._
import akka.actor.{ActorSystem, Actor, Props}
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpecLike}
import com.google.common.base.Stopwatch
import concurrent.duration._

object SimplePerfSpec {

  case class Finished(written: Int)

  class Writer(Until: Int, override val processorId: String) extends Processor {

    def receive = {
      case Persistent(payload, sequenceNr) =>
        print(s"$sequenceNr|")

      case Persistent(payload, Until) =>
        sender ! Finished(Until)
    }
  }

}


class SimplePerfSpec extends TestKit(ActorSystem("test")) with ImplicitSender with FlatSpecLike
  with Matchers with BeforeAndAfterAll {

  import SimplePerfSpec._

  val config = system.settings.config.getConfig("hbase-journal")

  behavior of "HBaseJournal"

  override protected def beforeAll() {
    HBaseJournalInit.createTable(config)
  }

  val messagesNr = 1234

  it should s"write $messagesNr messages" in {
    // given
    val msg = Persistent("Hello!")

    val writer = system.actorOf(Props(classOf[Writer], messagesNr, "w-1"))

    // when
    val stopwatch = timed {
      var i = 0
      while (i <= messagesNr) {
        writer ! msg
        i = i + 1
      }
    }

    within(60.seconds) {
      expectMsgType[Finished]
      info(s"Writing $messagesNr took: $stopwatch")
    }

    // then
  }

  def timed(block: => Unit): Stopwatch = {
    val stopwatch = (new Stopwatch).start()
    block
    stopwatch.stop()
  }
}