package akka.persistence.hbase.snapshot

import akka.testkit.{TestKit, ImplicitSender, TestProbe}
import akka.actor.{ActorLogging, Props, ActorRef, ActorSystem}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, DoNotDiscover}
import akka.persistence._
import akka.persistence.hbase.journal.{HBaseClientFactory, HBaseJournalInit}
import org.apache.hadoop.hbase.client.HBaseAdmin
import concurrent.duration._
import akka.persistence.SaveSnapshotFailure
import akka.persistence.SaveSnapshotSuccess
import akka.persistence.SnapshotMetadata
import com.typesafe.config.{ConfigFactory, Config}

object HadoopSnapshotStoreSpec {
  class SnapshottingActor(probe: ActorRef, override val processorId: String) extends Processor with ActorLogging {
    var data = List[String]()

    def receive = {
      // snapshot making ------------------------------------------------------

      case x: String =>
        log.info("Prepending: " + x)
        data ::= x

      case MakeSnapshot =>
        log.info("Starting snapshot creation: " + data)
        saveSnapshot(data)
        probe ! "making"

      case SaveSnapshotSuccess(meta) =>
        log.info("save success, desc = " + meta)
        probe ! SnapshotOk(meta)

      case SaveSnapshotFailure(meta, reason) =>
        log.info("failure: " + meta)
        probe ! SnapshotFail(meta, reason)

      // end of snapshot making -----------------------------------------------

      // snapshot offers ------------------------------------------------------

      case SnapshotOffer(metadata, offeredSnapshot) =>
        log.info("Offer: " + metadata + ", data: " + offeredSnapshot)
        data = offeredSnapshot.asInstanceOf[List[String]]
        probe ! WasOfferedSnapshot(data)
      
      case DeleteSnapshot(toSeqNr) =>
        log.info("Delete, to: " + toSeqNr)
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

trait HadoopSnapshotBehavior {
  self: TestKit with FlatSpecLike with BeforeAndAfterAll =>

  def config: Config

  import HadoopSnapshotStoreSpec._

  val hadoopSnapshotStore = {

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
      probe.expectMsg(max = 30.seconds, "making")
      val ok = probe.expectMsgType[SnapshotOk](max = 15.seconds)
      info(s"Snapshot successful: $ok")
    }

    it should "be offered a snapshot from the previous test (a, b, c)" in {
      // given
      val probe = TestProbe()
      val actor = system.actorOf(Props(classOf[SnapshottingActor], probe.ref, "snap-1"))

      // then
      probe.expectMsg(max = 20.seconds, WasOfferedSnapshot(List("c", "b", "a")))
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
  }
}



class HdfsSnapshotStoreSpec extends TestKit(ActorSystem("hdfs-test")) with FlatSpecLike with BeforeAndAfterAll
  with HadoopSnapshotBehavior {

  behavior of "HdfsSnapshotStore"

  def config: Config = ConfigFactory.parseString(
    s"""hadoop-snapshot-store.impl = "${classOf[HdfsSnapshotter].getCanonicalName}" """
  ).withFallback(system.settings.config)


  override protected def afterAll() {
    super.afterAll()
    system.shutdown()
  }

  it should behave like hadoopSnapshotStore

}


class HBaseSnapshotStoreSpec extends TestKit(ActorSystem("hbase-test")) with FlatSpecLike with BeforeAndAfterAll
  with HadoopSnapshotBehavior {

  behavior of "HBaseSnapshotStore"

  override protected def beforeAll() {
    HBaseJournalInit.createTable(config)
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

  def config: Config = ConfigFactory.parseString(
    s"""hadoop-snapshot-store.impl = "${classOf[HBaseSnapshotter].getCanonicalName}" """
  ).withFallback(system.settings.config)

  it should behave like hadoopSnapshotStore

}
