package akka.persistence.hbase.snapshot

import akka.actor.ActorSystem
import akka.persistence.{SelectedSnapshot, SnapshotSelectionCriteria}
import scala.concurrent.{Promise, Future}
import org.hbase.async.{KeyValue, HBaseClient}
import org.apache.hadoop.hbase.util.Bytes._
import akka.persistence.SnapshotMetadata
import akka.persistence.hbase.journal._
import akka.persistence.hbase.common._
import collection.JavaConverters._
import java.util. { ArrayList => JArrayList }
import scala.collection.immutable
import akka.persistence.serialization.Snapshot
import akka.serialization.SerializationExtension
import scala.util.{Failure, Success, Try}
import akka.persistence.hbase.common.TestingEventProtocol.DeletedSnapshotsFor

class HBaseSnapshotter(val system: ActorSystem, val hBasePersistenceSettings: PluginPersistenceSettings, val client: HBaseClient)
  extends HadoopSnapshotter
  with AsyncBaseUtils with DeferredConversions {

  val log = system.log

  implicit val settings = hBasePersistenceSettings

  implicit override val executionContext = system.dispatchers.lookup("akka-hbase-persistence-dispatcher")

  type AsyncBaseRows = JArrayList[JArrayList[KeyValue]]

  /** Snapshots we're in progress of saving */
  private var saving = immutable.Set.empty[SnapshotMetadata]

  import Columns._
  import RowTypeMarkers._

  def loadAsync(processorId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = {
    log.debug("Loading async for processorId: [{}] on criteria: {}", processorId, criteria)
    val scanner = newScanner()
    val SnapshotSelectionCriteria(maxSequenceNr, maxTimestamp) = criteria

    val start = RowKey.firstForProcessor(processorId)
    val stop = RowKey(processorId, maxSequenceNr)

    scanner.setStartKey(start.toBytes)
    scanner.setStopKey(stop.toBytes)
    scanner.setKeyRegexp(RowKey.patternForProcessor(processorId))

    val promise = Promise[Option[SelectedSnapshot]]()

    def completePromiseWithFirstDeserializedSnapshot(in: AnyRef): Unit = in match {
      case null =>
        promise trySuccess None // got to end of Scan, if nothing completed, we complete with "found no valid snapshot"
        scanner.close()
        log.debug("Finished async load for processorId: [{}] on criteria: {}", processorId, criteria)

      case rows: AsyncBaseRows =>
        val maybeSnapshot: Option[(Long, Snapshot)] = for {
          row      <- rows.asScala.headOption
          srow      = row.asScala
          seqNr     = bytesToVint(findColumn(srow, SequenceNr).value)
          snapshot <- deserialize(findColumn(srow, Message).value).toOption
        } yield seqNr -> snapshot

        maybeSnapshot match {
          case Some((seqNr, snapshot)) =>
            val selectedSnapshot = SelectedSnapshot(SnapshotMetadata(processorId, seqNr), snapshot.data)
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
          RowKey(meta.processorId, meta.sequenceNr).toBytes,
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
    executeDelete(RowKey(meta.processorId, meta.sequenceNr).toBytes)
  }

  def delete(processorId: String, criteria: SnapshotSelectionCriteria): Unit = {
    log.debug("Deleting processorId: [{}], criteria: {}", processorId, criteria)

    val scanner = newScanner()

    val start = RowKey.firstForProcessor(processorId)
    val stop = RowKey(processorId, criteria.maxSequenceNr)

    scanner.setStartKey(start.toBytes)
    scanner.setStopKey(stop.toBytes)
    scanner.setKeyRegexp(RowKey.patternForProcessor(processorId))

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
      case _ if settings.publishTestingEvents => system.eventStream.publish(DeletedSnapshotsFor(processorId, criteria))
    }
  }

}