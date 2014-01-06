package akka.persistence.journal.hbase

import akka.persistence._
import akka.testkit.{TestProbe, ImplicitSender, TestKit}
import akka.actor.{Actor, Props, ActorSystem}
import org.scalatest._
import scala.concurrent.duration._
import org.apache.hadoop.hbase.client.HBaseAdmin

object HBaseAsyncJournalSpec {

  case class Delete(sequenceNr: Long, permanent: Boolean)

  class ProcessorA(override val processorId: String) extends Processor {

    def receive = {
      case Persistent(payload, sequenceNr) =>
        sender ! payload
        sender ! sequenceNr
        sender ! recoveryRunning

      case Delete(sequenceNr, permanent) =>
        deleteMessage(sequenceNr, permanent)
    }
  }

  class ProcessorB(override val processorId: String) extends Processor {
    val destination = context.actorOf(Props[Destination])
    val channel = context.actorOf(Channel.props("channel"))

    def receive = {
      case p: Persistent => channel forward Deliver(p, destination)
    }
  }

  class Destination extends Actor {
    def receive = {
      case cp @ ConfirmablePersistent(payload, sequenceNr, _) =>
        sender ! s"$payload-$sequenceNr"
        cp.confirm()
    }
  }
}

class HBaseAsyncJournalSpec extends TestKit(ActorSystem("test")) with ImplicitSender with FlatSpecLike
  with Matchers with BeforeAndAfterAll {

  import HBaseAsyncJournalSpec._

  val config = system.settings.config.getConfig("hbase-journal")

  behavior of "HBaseJournal"

  override protected def beforeAll() {
    HBaseJournalInit.createTable(config)
  }

  it should "write and replay messages" in {
    val processor1 = system.actorOf(Props(classOf[ProcessorA], "p1"))

    processor1 ! Persistent("a")
    processor1 ! Persistent("aa")
    expectMsgAllOf(max = 5.seconds, "a", 1L, false)
    expectMsgAllOf(max = 5.seconds, "aa", 2L, false)

    val processor2 = system.actorOf(Props(classOf[ProcessorA], "p1"))
    processor2 ! Persistent("b")
    processor2 ! Persistent("c")
    expectMsgAllOf(max = 5.seconds, "a", 1L, true)
    expectMsgAllOf(max = 5.seconds, "aa", 2L, true)
    expectMsgAllOf(max = 5.seconds, "b", 3L, false)
    expectMsgAllOf(max = 5.seconds, "c", 4L, false)
  }

  it should "not replay messages marked as deleted" in {
    val deleteProbe = TestProbe()
    subscribeToDeletion(deleteProbe)

    val processor1 = system.actorOf(Props(classOf[ProcessorA], "p2"))
    processor1 ! Persistent("a")
    processor1 ! Persistent("b")
    expectMsgAllOf(max = 5.seconds, "a", 1L, false)
    expectMsgAllOf(max = 5.seconds, "b", 2L, false)
    processor1 ! Delete(1L, false)

    awaitDeletion(deleteProbe)

    system.actorOf(Props(classOf[ProcessorA], "p2"))
    expectMsgAllOf(max = 5.seconds, "b", 2L, true)
  }

  it should "not replay permanently deleted messages" in {
    val deleteProbe = TestProbe()
    subscribeToDeletion(deleteProbe)

    val processor1 = system.actorOf(Props(classOf[ProcessorA], "p3"))
    processor1 ! Persistent("a")
    processor1 ! Persistent("b")
    expectMsgAllOf(max = 5.seconds, "a", 1L, false)
    expectMsgAllOf(max = 5.seconds, "b", 2L, false)
    processor1 ! Delete(1L, true)
    awaitDeletion(deleteProbe)

    system.actorOf(Props(classOf[ProcessorA], "p3"))
    expectMsgAllOf("b", 2L, true)
  }

  it should "write delivery confirmations" in {
    val confirmProbe = TestProbe()
    subscribeToConfirmation(confirmProbe)

    val processor1 = system.actorOf(Props(classOf[ProcessorB], "p4"))
    1L to 8L foreach { i =>
      processor1 ! Persistent("a")
      awaitConfirmation(confirmProbe)
      expectMsg(s"a-$i")
    }

    val processor2 = system.actorOf(Props(classOf[ProcessorB], "p4"))
    processor2 ! Persistent("b")
    awaitConfirmation(confirmProbe)
    expectMsg("b-17")
  }


  def subscribeToConfirmation(probe: TestProbe): Unit =
    system.eventStream.subscribe(probe.ref, classOf[JournalProtocol.Confirm])

  def subscribeToDeletion(probe: TestProbe): Unit =
    system.eventStream.subscribe(probe.ref, classOf[JournalProtocol.Delete])

  def awaitConfirmation(probe: TestProbe): Unit =
    probe.expectMsgType[JournalProtocol.Confirm](max = 10.seconds)

  def awaitDeletion(probe: TestProbe): Unit =
    probe.expectMsgType[JournalProtocol.Delete](max = 10.seconds)

  override protected def afterAll() {
    val tableName = config.getString("table")
    println("Starting shutdown...")

    val admin = new HBaseAdmin(HBaseJournalInit.getHBaseConfig(config))
    admin.disableTable(tableName)
    admin.deleteTable(tableName)
    admin.close()

    system.shutdown()
  }
}
