package akka.persistence.hbase.snapshot

import akka.persistence.{SelectedSnapshot, SnapshotSelectionCriteria, SnapshotMetadata}
import scala.concurrent.Future
import akka.actor.{ActorSystem, Extension}
import scala.util.Try
import akka.persistence.serialization.Snapshot
import akka.serialization.SerializationExtension

/**
 * Common API for Snapshotter implementations. Used to provide an interface for the Extension.
 */
trait HadoopSnapshotter extends Extension {

  def system: ActorSystem

  val serialization = SerializationExtension(system)

  def loadAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]]

  def saveAsync(meta: SnapshotMetadata, snapshot: Any): Future[Unit]

  def saved(metadata: SnapshotMetadata): Unit

  def delete(metadata: SnapshotMetadata): Unit

  def delete(persistenceId: String, criteria: SnapshotSelectionCriteria): Unit

  protected def deserialize(bytes: Array[Byte]): Try[Snapshot] =
    serialization.deserialize(bytes, classOf[Snapshot])

  protected def serialize(snapshot: Snapshot): Try[Array[Byte]] =
    serialization.serialize(snapshot)


}
