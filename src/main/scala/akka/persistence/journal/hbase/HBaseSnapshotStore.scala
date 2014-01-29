package akka.persistence.journal.hbase

import akka.persistence.snapshot.SnapshotStore
import akka.persistence.{SnapshotMetadata, SelectedSnapshot, SnapshotSelectionCriteria}
import scala.concurrent.Future

class HBaseSnapshotStore extends SnapshotStore {

  def loadAsync(processorId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = ???

  def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] = ???

  def saved(metadata: SnapshotMetadata): Unit = ???

  def delete(metadata: SnapshotMetadata): Unit = ???

  def delete(processorId: String, criteria: SnapshotSelectionCriteria): Unit = ???
}
