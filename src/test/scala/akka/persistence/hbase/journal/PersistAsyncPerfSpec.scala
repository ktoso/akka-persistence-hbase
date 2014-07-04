package akka.persistence.hbase.journal

import java.util.Date
import java.util.concurrent.TimeUnit

import akka.actor.{ActorLogging, ActorRef, ActorSystem, Props}
import akka.persistence._
import akka.persistence.hbase.common.TestingEventProtocol.FinishedDeletes
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.google.common.base.Stopwatch
import org.apache.hadoop.hbase.client.{Scan, HTable}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.collection.JavaConversions
import scala.concurrent.duration._

object PersistAsyncPerfSpec {

  class Writer(untilSeqNr: Long, override val persistenceId: String) extends PersistentActor
    with ActorLogging {

    var lastPersisted: Any = _

    def receiveCommand = {
      case "ask" =>
        log.info("Got ASK command, lastPersisted = {}", lastPersisted)
        if (lastPersisted != null) {
          log.info("Replying with last persisted message {}", lastPersisted)
          sender() ! lastPersisted
        }

      case "delete" =>
        log.info("Deleting messages in {}, until {}", persistenceId, lastSequenceNr)
        deleteMessages(toSequenceNr = lastSequenceNr)

      case "boom" =>
        throw new RuntimeException("Boom!")

      case payload: AnyRef =>
        persistAsync(payload)(handlePersisted)
    }

    override def receiveRecover: Receive = {
      case m: AnyRef =>
        log.info("Recovered: {}", m)
        handlePersisted(m)
    }

    def handlePersisted(p: AnyRef): Unit = {
      if (!recoveryRunning) {
//        log.debug(s"persisted: {} @ {}", p, lastSequenceNr)
        sender() ! s"p-$p"
      }

      p match {
        case _: String => lastPersisted = p
        case RecoveryCompleted => context.system.eventStream.publish(p)
      }
    }
  }

}

//@DoNotDiscover
class PersistAsyncPerfSpec extends TestKit(ActorSystem("test")) with FlatSpecLike
  with ImplicitSender with Matchers with BeforeAndAfterAll {

  import akka.persistence.hbase.journal.PersistAsyncPerfSpec._

  lazy val config = system.settings.config

  lazy val pluginConfig = PluginPersistenceSettings(config)

  behavior of "HBaseJournal"

  val messagesNr = 2000

  val messages = (1 to messagesNr) map { i => s"hello-$i-(${new Date})" }
  
  var actor = createActor(messagesNr, "w-1")

  override def beforeAll() {
    HBaseJournalInit.createTable(config)
    super.beforeAll()
  }
  
  override def afterAll() {
    system.shutdown()
    system.awaitTermination(1.minute)
    super.afterAll()
  }

  it should s"write $messagesNr messages" in {
    val stopwatch = (new Stopwatch).start()
    
    messages foreach { m =>
      actor ! m
    }

    messagesNr.times { n => expectMsgType[String] should startWith (s"p-hello-$n") }
    stopwatch.stop()

    info(s"Sending/persisting $messagesNr messages took: $stopwatch time")
    info(s"This is ${messagesNr / stopwatch.elapsedTime(TimeUnit.MILLISECONDS)} msg/ms")
  }

  it should "replay those messages" in {
    val p = TestProbe()
    system.eventStream.subscribe(p.ref, classOf[RecoveryCompleted])

    val replayed = createActor(messagesNr, "w-1")

    replayed ! "ask"

    p.expectMsgType[RecoveryCompleted](30.seconds)

    val last = expectMsgType[String]
    last should startWith ("hello-")
  }

  it should "delete all messages up until that seq number" in {
    val p = TestProbe()
    system.eventStream.subscribe(p.ref, classOf[FinishedDeletes])

    actor ! "delete"

    p.expectMsgType[FinishedDeletes](max = 1.minute)

    countMessages() should equal (0)
  }

  private def countMessages(): Int = {
    val hTable = new HTable(pluginConfig.hadoopConfiguration, pluginConfig.table)
    val scan = new Scan
    val scanner = hTable.getScanner(scan)
    import JavaConversions._
    val count = scanner.iterator().foldLeft(0) { case (acc, _) => acc + 1}
    scanner.close()
    hTable.close()
    count
  }

  private def createActor(awaitMessages: Long, name: String): ActorRef =
    system.actorOf(Props(classOf[Writer], awaitMessages, name))

  implicit class TimesInt(i: Int) {
    def times(block: Int => Unit) = {
      1 to i foreach block
    }
  }

  def timed(block: => Unit): Stopwatch = {
    val stopwatch = (new Stopwatch).start()
    block
    stopwatch.stop()
  }
}
