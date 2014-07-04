package akka.persistence.hbase.snapshot

import java.util.{ArrayList => JArrayList}

import akka.actor.ActorSystem
import akka.persistence.hbase.common.TestingEventProtocol.DeletedSnapshotsFor
import akka.persistence.hbase.common._
import akka.persistence.hbase.journal._
import akka.persistence.serialization.Snapshot
import akka.persistence.{SelectedSnapshot, SnapshotMetadata, SnapshotSelectionCriteria}
import org.apache.hadoop.hbase.util.Bytes._
import org.hbase.async.{HBaseClient, KeyValue}

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}

class HBaseSnapshotter(val system: ActorSystem, val hBasePersistenceSettings: PluginPersistenceSettings, val client: HBaseClient)
  extends HadoopSnapshotter
  with AsyncBaseUtils with DeferredConversions {

  val log = system.log

  implicit val settings = hBasePersistenceSettings

  implicit override val pluginDispatcher = system.dispatchers.lookup("akka-hbase-persistence-dispatcher")

  type AsyncBaseRows = JArrayList[JArrayList[KeyValue]]

  /** Snapshots we're in progress of saving */
  private var saving = immutable.Set.empty[SnapshotMetadata]

  import akka.persistence.hbase.common.Columns._
  import akka.persistence.hbase.journal.RowTypeMarkers._

  def loadAsync(persistenceId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = {
    log.debug("Loading async for persistenceId: [{}] on criteria: {}", persistenceId, criteria)
    val scanner = newScanner()
    val SnapshotSelectionCriteria(maxSequenceNr, maxTimestamp) = criteria

    val start = RowKey.firstForPersistenceId(persistenceId)
    val stop = RowKey(selectPartition(maxSequenceNr), persistenceId, maxSequenceNr)

    scanner.setStartKey(start.toBytes)
    scanner.setStopKey(stop.toBytes)
    scanner.setKeyRegexp(RowKey.patternForProcessor(persistenceId))

    val promise = Promise[Option[SelectedSnapshot]]()

    def completePromiseWithFirstDeserializedSnapshot(in: AnyRef): Unit = in match {
      case null =>
        promise trySuccess None // got to end of Scan, if nothing completed, we complete with "found no valid snapshot"
        scanner.close()
        log.debug("Finished async load for persistenceId: [{}] on criteria: {}", persistenceId, criteria)

      case rows: AsyncBaseRows =>
        val maybeSnapshot: Option[(Long, Snapshot)] = for {
          row      <- rows.asScala.headOption
          srow      = row.asScala
          seqNr     = bytesToVint(findColumn(srow, SequenceNr).value)
          snapshot <- deserialize(findColumn(srow, Message).value).toOption
        } yield seqNr -> snapshot

        maybeSnapshot match {
          case Some((seqNr, snapshot)) =>
            val selectedSnapshot = SelectedSnapshot(SnapshotMetadata(persistenceId, seqNr), snapshot.data)
            promise success Some(selectedSnapshot)

          case None =>
            go()
        }
    }

    def go() = scanner.nextRows(1) map completePromiseWithFirstDeserializedSnapshot
    go()

    promise.future
  }

  def saveAsync(meta: SnapshotMetadata, snapshot: Any): Future[Unit] = {
    log.debug("Saving async, of {}", meta)
    saving += meta

    serialize(Snapshot(snapshot)) match {
      case Success(serializedSnapshot) =>
        executePut(
          RowKey(selectPartition(meta.sequenceNr), meta.persistenceId, meta.sequenceNr).toBytes,
          Array(Marker,              Message),
          Array(SnapshotMarkerBytes, serializedSnapshot)
        )

      case Failure(ex) =>
        Future failed ex
    }
  }

  def saved(meta: SnapshotMetadata): Unit = {
    log.debug("Saved: {}", meta)
    saving -= meta
  }

  def delete(meta: SnapshotMetadata): Unit = {
    log.debug("Deleting: {}", meta)
    saving -= meta
    executeDelete(RowKey(selectPartition(meta.sequenceNr), meta.persistenceId, meta.sequenceNr).toBytes)
  }

  def delete(persistenceId: String, criteria: SnapshotSelectionCriteria): Unit = {
    log.debug("Deleting persistenceId: [{}], criteria: {}", persistenceId, criteria)

    val scanner = newScanner()

    val start = RowKey.firstForPersistenceId(persistenceId)
    val stop = RowKey.lastForPersistenceId(persistenceId, criteria.maxSequenceNr)

    scanner.setStartKey(start.toBytes)
    scanner.setStopKey(stop.toBytes)
    scanner.setKeyRegexp(RowKey.patternForProcessor(persistenceId))

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