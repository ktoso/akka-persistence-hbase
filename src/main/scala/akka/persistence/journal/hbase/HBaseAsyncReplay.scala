package akka.persistence.journal.hbase

import akka.persistence.PersistentRepr
import akka.actor.{ActorLogging, Actor}
import scala.concurrent.Future
import org.hbase.async.{KeyValue, HBaseClient}
import java.util. { ArrayList => JArrayList }
import java.{util => ju}
import scala.collection.mutable
import org.apache.hadoop.hbase.util.Bytes

trait HBaseAsyncReplay {
  this: Actor with ActorLogging with HBaseJournalBase with PersistenceMarkers with DeferredConversions =>

  def replayDispatcherId: String
  private implicit lazy val replayDispatcher = context.system.dispatchers.lookup(replayDispatcherId)

  def client: HBaseClient

  import Columns._
  import collection.JavaConverters._

  def replayAsync(processorId: String, fromSequenceNr: Long, toSequenceNr: Long)(replayCallback: (PersistentRepr) => Unit): Future[Long] = {
    log.debug(s"Async replay for processorId [$processorId], from sequenceNr: [$fromSequenceNr], to sequenceNr: [$toSequenceNr]")

    val scanner = client.newScanner(Table)
    scanner.setFamily(Family)
    scanner.setStartKey(RowKey(processorId, fromSequenceNr).toBytes)
    scanner.setStopKey(RowKey(processorId, toSequenceNr).toBytes)
    scanner.setKeyRegexp(s""".*-$processorId-.*""")

    scanner.setMaxNumRows(scanBatchSize)

    val callback = replay(replayCallback) _

    def handleRows(in: AnyRef): Future[Long] = in match {
      case null =>
        log.debug("replayAsync - finished!")
        scanner.close()
        Future(0)

      case rows: JArrayList[JArrayList[KeyValue]] =>
        log.debug(s"replayAsync - got ${rows.size} rows...")

        val seqNrs = for {
          row <- rows.asScala
          cols = row.asScala
        } yield callback(cols)

        go() map { reachedSeqNr =>
          (reachedSeqNr :: seqNrs.toList).max
        }
    }

    def go() = scanner.nextRows() flatMap handleRows

    go()
  }

  private def replay(replayCallback: (PersistentRepr) => Unit)(columns: mutable.Buffer[KeyValue]): Long = {
    import Columns._

    def findColumn(qualifier: Array[Byte]) =
      columns find { kv =>
        ju.Arrays.equals(kv.qualifier, qualifier)
      } getOrElse {
        throw new RuntimeException(s"Unable to find [${Bytes.toString(qualifier)}}] field from: ${columns.map(kv => Bytes.toString(kv.qualifier))}")
      }

    val messageKetValue = findColumn(Message)
    var msg = persistentFromBytes(messageKetValue.value)

    val markerKeyValue = findColumn(Marker)
    val marker = Bytes.toString(markerKeyValue.value)

    marker match {
      case AcceptedMarker =>
        replayCallback(msg)

      case DeletedMarker =>
        msg = msg.update(deleted = true)

      case _ =>
        val channelId = extractSeqNrFromConfirmedMarker(marker)
        msg = msg.update(confirms = channelId +: msg.confirms)
        replayCallback(msg)
    }

    msg.sequenceNr
  }

}
