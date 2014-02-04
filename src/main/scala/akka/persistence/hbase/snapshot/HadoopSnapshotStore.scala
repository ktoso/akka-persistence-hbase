package akka.persistence.hbase.snapshot

import akka.persistence.snapshot.SnapshotStore
import akka.persistence.{PersistenceSettings, SelectedSnapshot, SnapshotSelectionCriteria}
import scala.concurrent.Future
import akka.persistence.SnapshotMetadata
import akka.actor.ActorLogging
import akka.contrib.persistence.hbase.common.DeferredConversions

class HadoopSnapshotStore extends SnapshotStore with ActorLogging
  with DeferredConversions {

  val snap = HadoopSnapshotterExtension(context.system)

  // todo deduplicate obtaining settings ======

  lazy val persistenceSettings = new PersistenceSettings(context.system.settings.config.getConfig("akka.persistence"))

  // todo deduplicate obtaining settings ======

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
  }}
