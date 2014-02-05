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
import com.typesafe.scalalogging.slf4j.Logging

class HBaseSnapshotter(system: ActorSystem, val hBasePersistenceSettings: HBasePersistenceSettings, val client: HBaseClient)
  extends HadoopSnapshotter with Logging
  with AsyncBaseUtils with DeferredConversions {

  private val serialization = SerializationExtension(system)

  implicit val settings = hBasePersistenceSettings

  implicit override val executionContext = system.dispatchers.lookup("akka-hbase-persistence-dispatcher")

  type AsyncBaseRows = JArrayList[JArrayList[KeyValue]]

  /** Snapshots we're in progress of saving */
  private var saving = immutable.Set.empty[SnapshotMetadata]

  import Columns._
  import RowTypeMarkers._

  println("Using Hbase snapshotter!!!")

  def loadAsync(processorId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = {
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

    promise.future
  }

  def saveAsync(meta: SnapshotMetadata, snapshot: Any): Future[Unit] = {
    logger.info("saveAsync - hbase impl")
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
    saving -= meta
    logger.info("saved! {}", meta)
  }

  def delete(meta: SnapshotMetadata): Unit = {
    saving -= meta

    // todo delete

    logger.info("deleted! {}", meta)
  }

  def delete(processorId: String, criteria: SnapshotSelectionCriteria): Unit = {
    ???
  }

  private def deserialize(bytes: Array[Byte]): Try[Snapshot] =
    serialization.deserialize(bytes, classOf[Snapshot])

  private def serialize(snapshot: Snapshot): Try[Array[Byte]] =
    serialization.serialize(snapshot)

}