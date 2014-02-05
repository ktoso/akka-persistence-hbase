package akka.persistence.hbase.snapshot

import akka.persistence.{SelectedSnapshot, SnapshotSelectionCriteria, SnapshotMetadata}
import scala.concurrent.Future

/**
* Dump and read Snapshots to/from HDFS.
*
* May be useful if snapshots are really big, or if you need easy "take out" of your data (simpler to get file fomr HDFS than a certin row as file for HBase).
*
* @todo implement me :-)
*/
class HdfsSnapshotter extends HadoopSnapshotter {

  def loadAsync(processorId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = ???

  def saveAsync(meta: SnapshotMetadata, snapshot: Any): Future[Unit] = ???

  def saved(metadata: SnapshotMetadata): Unit = ???

  def delete(metadata: SnapshotMetadata): Unit = ???

  def delete(processorId: String, criteria: SnapshotSelectionCriteria): Unit = ???
}
