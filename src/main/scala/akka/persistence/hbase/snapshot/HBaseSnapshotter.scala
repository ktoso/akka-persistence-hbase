package akka.persistence.hbase.snapshot

import akka.actor.Extension
import akka.persistence.{SelectedSnapshot, SnapshotSelectionCriteria, SnapshotMetadata}
import scala.concurrent.Future
import org.hbase.async.HBaseClient
import com.typesafe.config.Config

class HBaseSnapshotter(config: Config, client: HBaseClient) extends Extension with HadoopSnapshotter {

  def loadAsync(processorId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = ???

  def saveAsync(meta: SnapshotMetadata, snapshot: Any): Future[Unit] = ???

  def saved(metadata: SnapshotMetadata): Unit = ???

  def delete(metadata: SnapshotMetadata): Unit = ???

  def delete(processorId: String, criteria: SnapshotSelectionCriteria): Unit = ???
}
