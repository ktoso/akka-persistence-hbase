package akka.persistence.hbase.journal

import java.util.Date
import java.util.concurrent.TimeUnit

import akka.actor._
import akka.persistence._
import akka.persistence.hbase.common.TestingEventProtocol.FinishedDeletes
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.google.common.base.Stopwatch
import org.apache.hadoop.hbase.client.{Scan, HTable}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.collection.JavaConversions
import scala.concurrent.duration._

object PersistAsyncJournalPerfSpec {

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
      case r: RecoveryCompleted =>
        context.system.eventStream.publish(r)

      case m: AnyRef =>
        // log.info("Recovered: {}", m)
        handlePersisted(m)
    }

    def handlePersisted(p: AnyRef): Unit = {
      if (!recoveryRunning) {
        // log.debug(s"persisted: {} @ {}", p, lastSequenceNr)
        sender() ! s"p-$p"
      }

      lastPersisted = p
    }
  }

}

class PersistAsyncJournalPerfSpec extends TestKit(ActorSystem("test")) with FlatSpecLike
  with ImplicitSender with Matchers with BeforeAndAfterAll with BeforeAndAfter {

  import akka.persistence.hbase.journal.PersistAsyncJournalPerfSpec._

  lazy val config = system.settings.config

  lazy val settings = PersistencePluginSettings(config)

  behavior of "HBaseJournal"

  val messagesNr = 2000

  val messages = (1 to messagesNr) map { i => s"hello-$i-(${new Date})" }
  
  lazy val actor: ActorRef = createActor(messagesNr, "w-1")

  override def beforeAll() {
    HBaseJournalInit.createTable(config)
    super.beforeAll()
  }
  
  override def afterAll() {
    super.afterAll()
    
    HBaseJournalInit.disableTable(config, settings.table)
    HBaseJournalInit.deleteTable(config, settings.table)

    HBaseJournalInit.disableTable(config, settings.snapshotTable)
    HBaseJournalInit.deleteTable(config, settings.snapshotTable)
    
    HBaseClientFactory.reset()
    shutdown(system)
  }

  before {
    createActor(3, "warm-1") ! PoisonPill
  }

  it should s"write $messagesNr messages" in {
    val stopwatch = (new Stopwatch).start()
    
    messages foreach { m => actor ! m }

    messagesNr.times { n => expectMsgType[String](15.seconds) should startWith (s"p-hello-$n") }
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

    val last = expectMsgType[String](15.seconds)
    last should startWith ("hello-")
  }

  it should "delete all messages up until that seq number" in {
    val p = TestProbe()
    system.eventStream.subscribe(p.ref, classOf[FinishedDeletes])

    actor ! "delete"

    p.expectMsgType[FinishedDeletes](max = 30.seconds)

    countMessages() should equal (0)
  }

  private def countMessages(): Int = {
    val hTable = new HTable(settings.hadoopConfiguration, settings.table)
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
