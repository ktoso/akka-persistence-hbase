package akka.persistence.hbase.snapshot

import akka.testkit.{ImplicitSender, TestProbe, TestKit}
import akka.actor.{Props, ActorRef, ActorSystem}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}
import akka.persistence.{SnapshotMetadata, SaveSnapshotFailure, SaveSnapshotSuccess, Processor}
import akka.persistence.hbase.journal.HBaseJournalInit
import org.apache.hadoop.hbase.client.HBaseAdmin
import concurrent.duration._

object HadoopSnapshotStoreSpec {
  class SnapshottingActor(tellMe: ActorRef) extends Processor {
    var data = List[String]()

    def receive = {
      case x: String =>
        println("Prepending: " + x)
        data ::= x

      case MakeSnapshot =>
        println("Starting snapshot creation...")
        saveSnapshot(data)
        tellMe ! "making"

      case SaveSnapshotSuccess(meta) =>
        tellMe ! SnapshotOk(meta)

      case SaveSnapshotFailure(meta, reason) =>
        tellMe ! SnapshotFail(meta, reason)
    }
  }

  case object MakeSnapshot

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
    val actor = system.actorOf(Props(classOf[SnapshottingActor], probe.ref))

    // when
    actor ! "a"
    actor ! "b"
    actor ! "c"
    actor ! MakeSnapshot

    // then
    probe.expectMsg(max = 50.seconds, "making")
    val ok = probe.expectMsgType[SnapshotOk]
    info(s"Snapshot successful: $ok")
  }

  override protected def afterAll() {
    val tableName = config.getString("hbase-journal.table")

    val admin = new HBaseAdmin(HBaseJournalInit.getHBaseConfig(config))
    admin.disableTable(tableName)
    admin.deleteTable(tableName)
    admin.close()

    system.shutdown()
  }
}
