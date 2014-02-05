package akka.persistence.hbase.snapshot

import akka.testkit.{ImplicitSender, TestProbe, TestKit}
import akka.actor.{Props, ActorRef, ActorSystem}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}
import akka.persistence._
import akka.persistence.hbase.journal.{HBaseClientFactory, HBaseJournalInit}
import org.apache.hadoop.hbase.client.HBaseAdmin
import concurrent.duration._
import akka.persistence.SaveSnapshotFailure
import akka.persistence.SaveSnapshotSuccess
import akka.persistence.SnapshotMetadata

object HadoopSnapshotStoreSpec {
  class SnapshottingActor(probe: ActorRef, override val processorId: String) extends Processor {
    var data = List[String]()

    def receive = {
      // snapshot making ------------------------------------------------------

      case x: String =>
        println("Prepending: " + x)
        data ::= x

      case MakeSnapshot =>
        println("Starting snapshot creation: " + data)
        saveSnapshot(data)
        probe ! "making"

      case SaveSnapshotSuccess(meta) =>
        println("save success, meta = " + meta)
        probe ! SnapshotOk(meta)

      case SaveSnapshotFailure(meta, reason) =>
        println("failure: " + meta)
        probe ! SnapshotFail(meta, reason)

      // end of snapshot making -----------------------------------------------

      // snapshot offers ------------------------------------------------------

      case SnapshotOffer(metadata, offeredSnapshot) =>
        println("Offer: " + metadata + ", data: " + offeredSnapshot)
        data = offeredSnapshot.asInstanceOf[List[String]]
        probe ! WasOfferedSnapshot(data)
      
      case DeleteSnapshot(toSeqNr) =>
        println("Delete, to: " + toSeqNr)
        deleteSnapshot(toSeqNr, System.currentTimeMillis())

      // end of snapshot offers ------------------------------------------------
    }
  }

  case object MakeSnapshot
  case class DeleteSnapshot(toSeqNr: Long)
  case class WasOfferedSnapshot(data: List[String])

  case class SnapshotOk(meta: SnapshotMetadata)
  case class SnapshotFail(meta: SnapshotMetadata, reason: Throwable)
}

class HadoopSnapshotStoreSpec extends TestKit(ActorSystem("test")) with FlatSpecLike
  with BeforeAndAfterAll {

  behavior of "HadoopSnapshotStore"

  val config = system.settings.config

  import HadoopSnapshotStoreSpec._

  override protected def beforeAll() {
    HBaseJournalInit.createTable(config)
  }

  it should "store a snapshot" in {
    // given
    val probe = TestProbe()
    val actor = system.actorOf(Props(classOf[SnapshottingActor], probe.ref, "snap-1"))

    // when
    actor ! "a"
    actor ! "b"
    actor ! "c"
    actor ! MakeSnapshot

    // then
    probe.expectMsg(max = 10.seconds, "making")
    val ok = probe.expectMsgType[SnapshotOk](max = 10.seconds)
    info(s"Snapshot successful: $ok")
  }

  it should "be offered a snapshot from the previous test (a, b, c)" in {
    // given
    val probe = TestProbe()
    val actor = system.actorOf(Props(classOf[SnapshottingActor], probe.ref, "snap-1"))

    // then
    probe.expectMsg(max = 10.seconds, WasOfferedSnapshot(List("c", "b", "a")))
  }

  it should "be able to delete a snapshot, so it won't be replayed again" in {
    // given
    val probe = TestProbe()
    val actor = system.actorOf(Props(classOf[SnapshottingActor], probe.ref, "snap-1"))
    Thread.sleep(1000)

    // when
    actor ! DeleteSnapshot(3)
    Thread.sleep(1000)

    // then
    val actor2 = system.actorOf(Props(classOf[SnapshottingActor], probe.ref, "snap-1"))
    Thread.sleep(1000)
    expectNoMsg(2.seconds) // we deleted the snapshot, nothing there to replay

    actor2 ! "d"
    expectNoMsg(max = 5.seconds)

    val actor3 = system.actorOf(Props(classOf[SnapshottingActor], probe.ref, "snap-1"))
    expectNoMsg(max = 5.seconds) // we didn't snapshot, and it's not persistent
  }

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
