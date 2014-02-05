package akka.persistence.hbase.snapshot

import akka.persistence.snapshot.SnapshotStore
import akka.persistence.{SelectedSnapshot, SnapshotSelectionCriteria}
import scala.concurrent.Future
import akka.persistence.SnapshotMetadata
import akka.actor.ActorLogging

class HadoopSnapshotStore extends SnapshotStore with ActorLogging {

  println("snapshot store!!!!")

  val snap = HadoopSnapshotterExtensionId(context.system)

  def loadAsync(processorId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = {
    snap.loadAsync(processorId: String, criteria: SnapshotSelectionCriteria)
  }

  def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] = {
    log.info("SaveAsync of snapshot with metadata: {}", metadata)

    snap.saveAsync(metadata, snapshot)
  }

  def saved(metadata: SnapshotMetadata): Unit = {
    snap.saved(metadata)
  }

  def delete(metadata: SnapshotMetadata): Unit = {
    snap.delete(metadata)
  }

  def delete(processorId: String, criteria: SnapshotSelectionCriteria): Unit = {
    snap.delete(processorId, criteria)
  }
}
