package akka.persistence.hbase.journal

import akka.actor.{ActorLogging, ActorRef, ActorSystem, Props}
import akka.persistence._
import akka.persistence.hbase.common.TestingEventProtocol.FinishedDeletes
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.scalatest._

import scala.concurrent.duration._

object HBaseAsyncJournalSpec {

  case class DeleteUntil(sequenceNr: Long, permanent: Boolean)

  class MyPersistentActor(testActor: ActorRef, override val persistenceId: String) extends PersistentActor with ActorLogging {

    val handler: Receive = {
      case DeleteUntil(nr, permanent) =>
        log.debug("Deleting messages until {}, permanent: {}", nr, permanent)
        deleteMessages(toSequenceNr = nr, permanent)

      case RecoveryCompleted => // do nothing...

      case payload if recoveryRunning =>
        log.debug("Recovering, got {} @ {} ({})", payload, lastSequenceNr, getCurrentPersistentMessage)
        sender() ! payload
        sender() ! lastSequenceNr
        sender() ! recoveryRunning

      case payload =>
        persist(payload) { p =>
          log.debug("Not in recovery, got {} @ {}", payload, lastSequenceNr)
          testActor ! payload
          testActor ! lastSequenceNr
          testActor ! recoveryRunning
        }
    }

    def receiveCommand = handler

    def receiveRecover = handler
  }

}

class HBaseAsyncJournalSpec extends TestKit(ActorSystem("test")) with ImplicitSender with FlatSpecLike
with Matchers with BeforeAndAfterAll {

  import akka.persistence.hbase.journal.HBaseAsyncJournalSpec._

  val config = system.settings.config

  behavior of "HBaseJournal"

  val timeout = 5.seconds

  override protected def beforeAll() {
    HBaseJournalInit.createTable(config)
  }

  it should "write and replay messages" in {
    val processor1 = system.actorOf(Props(classOf[MyPersistentActor], self, "p1"))
    info("p1 = " + processor1)

    processor1 ! "a"
    processor1 ! "aa"
    expectMsgAllOf(max = timeout, "a", 1L, false)
    expectMsgAllOf(max = timeout, "aa", 2L, false)

    val processor2 = system.actorOf(Props(classOf[MyPersistentActor], self, "p1"))
    processor2 ! "b"
    processor2 ! "c"
    expectMsgAllOf(max = timeout, "a", 1L, true)
    expectMsgAllOf(max = timeout, "aa", 2L, true)
    expectMsgAllOf(max = timeout, "b", 3L, false)
    expectMsgAllOf(max = timeout, "c", 4L, false)
  }

  it should "not replay messages marked as deleted" in {
    val deleteProbe = TestProbe()
    subscribeToDeletion(deleteProbe)

    val processor1 = system.actorOf(Props(classOf[MyPersistentActor], self, "p2"))
    processor1 ! "a"
    processor1 ! "b"
    expectMsgAllOf(max = timeout, "a", 1L, false)
    expectMsgAllOf(max = timeout, "b", 2L, false)
    processor1 ! DeleteUntil(1L, permanent = false)

    awaitDeletion(deleteProbe)

    system.actorOf(Props(classOf[MyPersistentActor], self, "p2"))
    expectMsgAllOf(max = timeout, "b", 2L, true)
  }

  it should "not replay permanently deleted messages" in {
    val deleteProbe = TestProbe()
    subscribeToDeletion(deleteProbe)

    val processor1 = system.actorOf(Props(classOf[MyPersistentActor], self, "p3"))
    processor1 ! "a"
    processor1 ! "b"
    expectMsgAllOf(max = timeout, "a", 1L, false)
    expectMsgAllOf(max = timeout, "b", 2L, false)
    processor1 ! DeleteUntil(1L, permanent = true)
    awaitDeletion(deleteProbe)

    system.actorOf(Props(classOf[MyPersistentActor], self, "p3"))
    expectMsgAllOf("b", 2L, true)
  }

  // is assured, but no test yet
  it should "don't apply snapshots the same way as messages" in pending


  def subscribeToDeletion(probe: TestProbe): Unit =
    system.eventStream.subscribe(probe.ref, classOf[FinishedDeletes])

  def awaitDeletion(probe: TestProbe): Unit =
    probe.expectMsgType[FinishedDeletes](max = 10.seconds)

  override protected def afterAll() {
    val tableName = config.getString("hbase-journal.table")

    val admin = new HBaseAdmin(HBaseJournalInit.getHBaseConfig(config))
    admin.disableTable(tableName)
    admin.deleteTable(tableName)
    admin.close()

    HBaseClientFactory.reset()

    system.shutdown()
  }
}
