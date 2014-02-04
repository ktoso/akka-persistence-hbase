package akka.contrib.persistence.hbase.snapshot

import akka.testkit.{TestProbe, TestKit}
import akka.actor.{Props, ActorRef, ActorSystem}
import org.scalatest.FlatSpecLike
import akka.persistence.{SnapshotMetadata, SaveSnapshotFailure, SaveSnapshotSuccess, Processor}

object HBaseSnapshotStoreTest {
  class SnapshottingActor(tellMe: ActorRef) extends Processor {
    var data = List[String]()

    def receive = {
      case x: String =>
        data ::= x

      case MakeSnapshot =>
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

class HBaseSnapshotStoreTest extends TestKit(ActorSystem("snapshot-test")) with FlatSpecLike {

  behavior of "HBaseSnapshotStore"

  import HBaseSnapshotStoreTest._

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
    probe.expectMsg("making")
    val ok = probe.expectMsgType[SnapshotOk]
    info(s"Snapshot successful: $ok")
  }


}
