package akka.persistence.hbase.snapshot

import akka.persistence.{SelectedSnapshot, SnapshotSelectionCriteria, SnapshotMetadata}
import scala.concurrent.Future
import akka.actor.Extension

/**
 * Common API for Snapshotter implementations. Used to provide an interface for the Extension.
 */
trait HadoopSnapshotter extends Extension {

  def loadAsync(processorId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]]

  def saveAsync(meta: SnapshotMetadata, snapshot: Any): Future[Unit]

  def saved(metadata: SnapshotMetadata): Unit

  def delete(metadata: SnapshotMetadata): Unit

  def delete(processorId: String, criteria: SnapshotSelectionCriteria): Unit

}
