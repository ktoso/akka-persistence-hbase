package akka.persistence.hbase.journal

import akka.actor.{ActorLogging, ActorRef, ActorSystem, Props}
import akka.persistence._
import akka.persistence.hbase.common.TestingEventProtocol.FinishedDeletes
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
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
        log.debug("Recovering, got [{}] @ {} ({})", payload, currentPersistentMessage.map(_.sequenceNr).get, getCurrentPersistentMessage)
        testActor ! payload
        testActor ! currentPersistentMessage.map(_.sequenceNr).get
        testActor ! recoveryRunning

      case payload =>
        log.debug("Got payload {}, persisting...", payload)

        persist(payload) { p =>
          log.debug("Not in recovery, got [{}] @ {}", payload, currentPersistentMessage.map(_.sequenceNr).get)
          testActor ! payload
          testActor ! currentPersistentMessage.map(_.sequenceNr).get
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

  val pluginSettings = PersistencePluginSettings(config)

  behavior of "HBaseJournal"

  val timeout = 5.seconds

  override protected def beforeAll() {
    super.beforeAll()
    HBaseJournalInit.createTable(config, pluginSettings.table, pluginSettings.family)
    HBaseJournalInit.createTable(config, pluginSettings.snapshotTable, pluginSettings.snapshotFamily)

    Thread.sleep(2000)
  }

  it should "write and replay messages" in {
    val a1 = system.actorOf(Props(classOf[MyPersistentActor], self, "p1"))

    a1 ! "a"
    a1 ! "aa"
    expectMsgAllOf(max = timeout, "a", 1L, false)
    expectMsgAllOf(max = timeout, "aa", 2L, false)

    val a2 = system.actorOf(Props(classOf[MyPersistentActor], self, "p1"))
    a2 ! "b"
    a2 ! "c"
    expectMsgAllOf(max = timeout, "a", 1L, true)
    expectMsgAllOf(max = timeout, "aa", 2L, true)
    expectMsgAllOf(max = timeout, "b", 3L, false)
    expectMsgAllOf(max = timeout, "c", 4L, false)
  }

  it should "not replay permanently deleted messages" in {
    val deleteProbe = TestProbe()
    subscribeToDeletion(deleteProbe)

    val a1 = system.actorOf(Props(classOf[MyPersistentActor], self, "p2"))
    a1 ! "a"
    a1 ! "b"
    a1 ! "c"
    a1 ! "d"
    expectMsgAllOf(max = timeout, "a", 1L, false)
    expectMsgAllOf(max = timeout, "b", 2L, false)
    expectMsgAllOf(max = timeout, "c", 3L, false)
    expectMsgAllOf(max = timeout, "d", 4L, false)
    a1 ! DeleteUntil(2L, permanent = true)
    awaitDeletion(deleteProbe)

    val a2 = system.actorOf(Props(classOf[MyPersistentActor], self, "p2"))
    a2 ! "e"
    expectMsgAllOf("c", 3L, true)
    expectMsgAllOf("d", 4L, true)
    expectMsgAllOf("e", 5L, false)
  }

  it should "not replay messages marked as deleted" in {
    val deleteProbe = TestProbe()
    subscribeToDeletion(deleteProbe)

    val a = system.actorOf(Props(classOf[MyPersistentActor], self, "p3"))
    a ! "a"
    a ! "b"
    expectMsgAllOf(max = timeout, "a", 1L, false)
    expectMsgAllOf(max = timeout, "b", 2L, false)
    a ! DeleteUntil(1L, permanent = false)

    awaitDeletion(deleteProbe)

    system.actorOf(Props(classOf[MyPersistentActor], self, "p3"))
    expectMsgAllOf(max = timeout, "b", 2L, true)
  }

  lazy val settings = PersistencePluginSettings(system.settings.config)

  it should "delete exactly as much as needed messages" in {
    val deleteProbe = TestProbe()
    subscribeToDeletion(deleteProbe)

    val a = system.actorOf(Props(classOf[MyPersistentActor], self, "p4"))

    val partitionRange = 1 to settings.partitionCount

    partitionRange foreach { i =>
      a ! s"msg-$i"
      expectMsgAllOf(max = timeout, s"msg-$i", i.toLong, false)
    }
    a ! "next-1"
    a ! "next-2"
    a ! "next-3"

    a ! DeleteUntil(partitionRange.size, permanent = true)
    awaitDeletion(deleteProbe)

    val p2 = TestProbe()
    system.actorOf(Props(classOf[MyPersistentActor], p2.ref, "p4"))
    p2.fishForMessage(max = 2.minute, hint = "next-messages") {
      case "next-1" => false
      case "next-2" => false
      case "next-3" => true
      case b: Boolean => false
      case n: Long => false
    }
  }

//  is assured, but no test yet
  it should "don't apply snapshots the same way as messages" in pending


  def subscribeToDeletion(probe: TestProbe): Unit =
    system.eventStream.subscribe(probe.ref, classOf[FinishedDeletes])

  def awaitDeletion(probe: TestProbe): Unit =
    probe.expectMsgType[FinishedDeletes](max = 10.seconds)

  override protected def afterAll() {
    HBaseJournalInit.disableTable(config, pluginSettings.table)
    HBaseJournalInit.deleteTable(config, pluginSettings.table)

    HBaseJournalInit.disableTable(config, pluginSettings.snapshotTable)
    HBaseJournalInit.deleteTable(config, pluginSettings.snapshotTable)

    HBaseClientFactory.reset()

    shutdown(system)

    super.afterAll()
  }
}
