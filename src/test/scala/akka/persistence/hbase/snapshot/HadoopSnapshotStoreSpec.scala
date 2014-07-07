package akka.persistence.hbase.snapshot

import akka.actor.{ActorLogging, ActorRef, ActorSystem, Props}
import akka.persistence._
import akka.persistence.hbase.journal.{HBaseClientFactory, HBaseJournalInit, PersistencePluginSettings}
import akka.testkit.{TestKit, TestProbe}
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{BeforeAndAfterAll, DoNotDiscover, FlatSpecLike, Matchers}

import scala.concurrent.duration._

object HadoopSnapshotStoreSpec {
  class SnapshottingActor(probe: ActorRef, override val persistenceId: String) extends PersistentActor with ActorLogging {
    var data = List[String]()

    val commands: Receive = {
      case x: String =>
        persist(x) { _ =>
          log.info("Prepending: " + x)
          data ::= x
        }

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
    }

    val snapshots: Receive = {
      case SnapshotOffer(metadata, offeredSnapshot) =>
        log.info("Offer: " + metadata + ", data: " + offeredSnapshot)
        data = offeredSnapshot.asInstanceOf[List[String]]
        probe ! WasOfferedSnapshot(data)

      case DeleteSnapshot(toSeqNr) =>
        log.info("Delete, snapshot: " + toSeqNr)
        deleteSnapshot(toSeqNr, System.currentTimeMillis())
    }

    override def receiveCommand = commands orElse snapshots

    override def receiveRecover = receiveCommand  

  }

  class LastValueSnapshottingActor(probe: ActorRef, override val persistenceId: String) extends PersistentActor with ActorLogging {

    var last: String = ""

    val commands: Receive = {
      case "ask" =>
        sender() ! last

      case x: String =>
        persist(x) { x =>
          last = x
          log.info("Persisted last = {}", last)
        }

      case MakeSnapshot =>
        val data = "snap-" + last
        log.info("Saving snapshot: {}", data)

        saveSnapshot(data)

      case SaveSnapshotSuccess(meta) =>
        log.info("Snapshot save success, desc = " + meta)
        probe ! SnapshotOk(meta)

      case SaveSnapshotFailure(meta, reason) =>
        log.info("Snapshot save failure: " + meta)
        probe ! SnapshotFail(meta, reason)
    }

    val snapshots: Receive = {
      case SnapshotOffer(metadata, offeredSnapshot) =>
        log.info("Got SnapshotOffer: " + metadata + ", data: " + offeredSnapshot)
        last = offeredSnapshot.asInstanceOf[String]
        probe ! WasOfferedSnapshot(last)

      case DeleteSnapshot(toSeqNr) =>
        log.info("Delete, snapshot: " + toSeqNr)
        deleteSnapshot(toSeqNr, System.currentTimeMillis())
    }

    override def receiveCommand = commands orElse snapshots

    override def receiveRecover = receiveCommand

  }

  case object MakeSnapshot
  case class DeleteSnapshot(toSeqNr: Long)
  case class WasOfferedSnapshot(data: Any)

  case class SnapshotOk(meta: SnapshotMetadata)
  case class SnapshotFail(meta: SnapshotMetadata, reason: Throwable)
}

trait HadoopSnapshotBehavior {
  self: TestKit with Matchers with FlatSpecLike with BeforeAndAfterAll =>

  def config: Config

  import akka.persistence.hbase.snapshot.HadoopSnapshotStoreSpec._

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

    it should "be offered a snapshot from the previous test (c, b, a)" in {
      // given
      val probe = TestProbe()
      val snap1 = system.actorOf(Props(classOf[SnapshottingActor], probe.ref, "snap-1"))

      // then
      probe.expectMsg(max = 20.seconds, WasOfferedSnapshot(List("c", "b", "a")))
    }

    it should "be offered only the latest snapshot" in {
      val probe = TestProbe()
      val snap2 = system.actorOf(Props(classOf[LastValueSnapshottingActor], probe.ref, "snap-2"))

      snap2 ! "a"
      snap2 ! MakeSnapshot
      snap2 ! "b"
      snap2 ! MakeSnapshot
      snap2 ! "c"
      snap2 ! MakeSnapshot

      probe.expectMsgType[SnapshotOk](max = 10.seconds)
      probe.expectMsgType[SnapshotOk](max = 10.seconds)
      probe.expectMsgType[SnapshotOk](max = 10.seconds)

      val probe2 = TestProbe()
      val snap22 = system.actorOf(Props(classOf[LastValueSnapshottingActor], probe2.ref, "snap-2"))

      val WasOfferedSnapshot(msg: String) = probe2.expectMsgType[WasOfferedSnapshot](max = 20.seconds)
      msg should equal ("snap-c")
    }

    it should "be able to delete a snapshot, so it won't be replayed again" in {
      // given
      val probe = TestProbe()
      val snap1 = system.actorOf(Props(classOf[SnapshottingActor], probe.ref, "snap-1"))
      Thread.sleep(1000)

      // when
      snap1 ! DeleteSnapshot(3)
      Thread.sleep(1000)

      // then
      val actor2 = system.actorOf(Props(classOf[SnapshottingActor], probe.ref, "snap-1"))
      expectNoMsg(2.seconds) // we deleted the snapshot, nothing there to replay

      actor2 ! "d"
      expectNoMsg(max = 3.seconds)

      val actor3 = system.actorOf(Props(classOf[SnapshottingActor], probe.ref, "snap-1"))
      expectNoMsg(max = 3.seconds) // we didn't snapshot, and it's not persistent
    }
  }
}

@DoNotDiscover
@deprecated("Will be moved outside as separate project")
class HdfsSnapshotStoreSpec extends TestKit(ActorSystem("hdfs-test")) with FlatSpecLike with Matchers
  with BeforeAndAfterAll
  with HadoopSnapshotBehavior {

  behavior of "HdfsSnapshotStore"

  def config =
    ConfigFactory.parseString("mode = hdfs")
      .withFallback(system.settings.config)


  override protected def afterAll() {
    super.afterAll()
    shutdown(system)
  }

  it should behave like hadoopSnapshotStore

}


class HBaseSnapshotStoreSpec extends TestKit(ActorSystem("hbase-test")) with FlatSpecLike with Matchers
  with BeforeAndAfterAll
  with HadoopSnapshotBehavior {

  behavior of "HBaseSnapshotStore"

  val pluginSettings = PersistencePluginSettings(config)

  override protected def beforeAll() {
    HBaseJournalInit.createTable(config, pluginSettings.table, pluginSettings.family)
    HBaseJournalInit.createTable(config, pluginSettings.snapshotTable, pluginSettings.snapshotFamily)
    super.beforeAll()
  }

  override protected def afterAll() {
    super.afterAll()

    HBaseJournalInit.disableTable(config, pluginSettings.table)
    HBaseJournalInit.deleteTable(config, pluginSettings.table)

    HBaseJournalInit.disableTable(config, pluginSettings.snapshotTable)
    HBaseJournalInit.deleteTable(config, pluginSettings.snapshotTable)

    HBaseClientFactory.reset()

    shutdown(system)
  }

  def config =
    ConfigFactory.parseString("mode = hbase")
      .withFallback(system.settings.config)

  it should behave like hadoopSnapshotStore

}
