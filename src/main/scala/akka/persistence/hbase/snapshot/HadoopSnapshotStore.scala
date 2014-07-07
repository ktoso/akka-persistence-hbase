package akka.persistence.hbase.snapshot

import akka.persistence.snapshot.SnapshotStore
import akka.persistence.{SelectedSnapshot, SnapshotSelectionCriteria}
import scala.concurrent.Future
import akka.persistence.SnapshotMetadata
import akka.actor.ActorLogging

class HadoopSnapshotStore extends SnapshotStore with ActorLogging {

  val snap = HadoopSnapshotterExtension(context.system)

  def loadAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] =
    snap.loadAsync(persistenceId, criteria)

  def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] =
    snap.saveAsync(metadata, snapshot)

  def saved(metadata: SnapshotMetadata): Unit =
    snap.saved(metadata)

  def delete(metadata: SnapshotMetadata): Unit =
    snap.delete(metadata)

  def delete(persistenceId: String, criteria: SnapshotSelectionCriteria): Unit =
    snap.delete(persistenceId, criteria)
}

