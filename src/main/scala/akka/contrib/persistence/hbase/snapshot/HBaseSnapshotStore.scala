package akka.contrib.persistence.hbase.snapshot

import akka.persistence.snapshot.SnapshotStore
import akka.persistence.{PersistenceSettings, SelectedSnapshot, SnapshotSelectionCriteria}
import scala.concurrent.Future
import akka.contrib.persistence.hbase.journal.{HBaseJournalBase, HBaseClientFactory, HBaseJournalInit}
import HBaseJournalInit._
import akka.persistence.SnapshotMetadata
import akka.actor.ActorLogging
import org.hbase.async.PutRequest
import akka.contrib.persistence.hbase.common.{DeferredConversions, HBaseSerialization}

class HBaseSnapshotStore extends SnapshotStore with ActorLogging
  with HBaseJournalBase
  with DeferredConversions {

  import Columns._

  // todo deduplicate obtaining settings ======

  lazy val persistenceSettings = new PersistenceSettings(context.system.settings.config.getConfig("akka.persistence"))

  // todo deduplicate obtaining settings ======

  val client = HBaseClientFactory.getClient(journalConfig, persistenceSettings)

  def loadAsync(processorId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = ???

  def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] = {
    log.info("SaveAsync of snapshot with metadata: {}", metadata)

    saveSnapshot(metadata, snapshot)
  }

  def saved(metadata: SnapshotMetadata): Unit = ???

  def delete(metadata: SnapshotMetadata): Unit = ???

  def delete(processorId: String, criteria: SnapshotSelectionCriteria): Unit = ???

  // ----------------------

  // todo separate out

  def saveSnapshot(meta: SnapshotMetadata, snapshot: Any): Future[Unit] = {
    val rowKey = RowKey(meta.processorId, meta.sequenceNr)
    new PutRequest(TableBytes, rowKey.toBytes, Fami)
  }


}
