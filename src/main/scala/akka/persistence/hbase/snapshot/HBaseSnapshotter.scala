package akka.persistence.hbase.snapshot

import java.util.{ArrayList => JArrayList}

import akka.actor.ActorSystem
import akka.persistence.hbase.common.TestingEventProtocol.DeletedSnapshotsFor
import akka.persistence.hbase.common._
import akka.persistence.hbase.journal._
import akka.persistence.serialization.Snapshot
import akka.persistence.{SelectedSnapshot, SnapshotMetadata, SnapshotSelectionCriteria}
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.util.Bytes
import org.hbase.async.{HBaseClient, KeyValue}

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.concurrent.Future
import scala.util.{Failure, Success}

class HBaseSnapshotter(val system: ActorSystem, val hBasePersistenceSettings: PersistencePluginSettings, val client: HBaseClient)
  extends HadoopSnapshotter
  with HBaseUtils with AsyncBaseUtils with HBaseSerialization
  with DeferredConversions {

  val log = system.log

  implicit val settings = hBasePersistenceSettings

  lazy val table = hBasePersistenceSettings.snapshotTable

  lazy val family = hBasePersistenceSettings.snapshotFamily

  lazy val hTable = new HTable(settings.hadoopConfiguration, tableBytes)

  implicit override val pluginDispatcher = system.dispatchers.lookup("akka-hbase-persistence-dispatcher")

  type AsyncBaseRows = JArrayList[JArrayList[KeyValue]]

  /** Snapshots we're in progress of saving */
  private var saving = immutable.Set.empty[SnapshotMetadata]

  import akka.persistence.hbase.common.Columns._
  import akka.persistence.hbase.journal.RowTypeMarkers._

  def loadAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = {
    log.debug("Loading async for persistenceId: [{}] on criteria: {}", persistenceId, criteria)

    def scanPartition(): Option[SelectedSnapshot] = {
      val startScanKey = SnapshotRowKey.lastForPersistenceId(persistenceId, toSequenceNr = criteria.maxSequenceNr)
      val stopScanKey = persistenceId

      val scan = preparePrefixScan(tableBytes, familyBytes, startScanKey.toBytes, Bytes.toBytes(stopScanKey), persistenceId, onlyRowKeys = false)
      scan.addColumn(familyBytes, Message)
      scan.setReversed(true)
      scan.setMaxResultSize(1)
      val scanner = hTable.getScanner(scan)

      try {
        var res = scanner.next()
        while (res != null) {
          val seqNr = RowKey.extractSeqNr(res.getRow)
          val messageCell = res.getColumnLatestCell(familyBytes, Message)

          val snapshot = snapshotFromBytes(CellUtil.cloneValue(messageCell))

          if (seqNr <= criteria.maxSequenceNr)
            return Some(SelectedSnapshot(SnapshotMetadata(persistenceId, seqNr), snapshot.data)) // todo timestamp)

          res = scanner.next()
        }

        None
      } finally {
        scanner.close()
      }
    }

    val f = Future(scanPartition())
    f onFailure { case x => log.error(x, "Unable to read snapshot for persistenceId: {}, on criteria: {}", persistenceId, criteria) }
    f
  }

  def saveAsync(meta: SnapshotMetadata, snapshot: Any): Future[Unit] = {
    log.debug("Saving async, of {}", meta)
    saving += meta

    serialize(Snapshot(snapshot)) match {
      case Success(serializedSnapshot) =>
        executePut(
          SnapshotRowKey(meta.persistenceId, meta.sequenceNr).toBytes,
          Array(Marker,              Message),
          Array(SnapshotMarkerBytes, serializedSnapshot)
        )

      case Failure(ex) =>
        Future failed ex
    }
  }

  def saved(meta: SnapshotMetadata): Unit = {
    log.debug("Saved snapshot for meta: {}", meta)
    saving -= meta
  }

  def delete(meta: SnapshotMetadata): Unit = {
    log.debug("Deleting snapshot for meta: {}", meta)
    saving -= meta
    executeDelete(SnapshotRowKey(meta.persistenceId, meta.sequenceNr).toBytes)
  }

  def delete(persistenceId: String, criteria: SnapshotSelectionCriteria): Unit = {
    log.debug("Deleting snapshot for persistenceId: [{}], criteria: {}", persistenceId, criteria)

    val scanner = newScanner()

    val start = SnapshotRowKey.firstForPersistenceId(persistenceId)
    val stopSequenceNr = if (criteria.maxSequenceNr < Long.MaxValue) criteria.maxSequenceNr + 1 else Long.MaxValue
    val stop = SnapshotRowKey.lastForPersistenceId(persistenceId, stopSequenceNr)

    scanner.setStartKey(start.toBytes)
    scanner.setStopKey(stop.toBytes)
    scanner.setKeyRegexp(s"""$persistenceId-.*""")

    def handleRows(in: AnyRef): Future[Unit] = in match {
      case null =>
        log.debug("Finished scanning for snapshots to delete")
        flushWrites()
        scanner.close()
        Future.successful()

      case rows: AsyncBaseRows =>
        val deletes = for {
          row <- rows.asScala
          col <- row.asScala.headOption
          if isSnapshotRow(row.asScala)
        } yield deleteRow(col.key)

        go() flatMap { _ => Future.sequence(deletes) }
    }

    def go(): Future[Unit] = scanner.nextRows() flatMap handleRows

    go() map {
      case _ if settings.publishTestingEvents => system.eventStream.publish(DeletedSnapshotsFor(persistenceId, criteria))
    }
  }

}