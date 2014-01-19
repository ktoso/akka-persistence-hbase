package akka.persistence.journal.hbase

import akka.persistence.PersistentRepr
import akka.actor.{ActorLogging, Actor}
import scala.concurrent.Future
import org.hbase.async.{KeyValue, HBaseClient}
import java.util. { ArrayList => JArrayList }
import java.{util => ju}
import scala.collection.mutable
import org.apache.hadoop.hbase.util.Bytes
import akka.persistence.journal.japi.AsyncRecovery

trait HBaseAsyncRecovery extends AsyncRecovery with DeferredConversions {
  this: Actor with ActorLogging with HBaseJournalBase with PersistenceMarkers =>

  def client: HBaseClient

  def journalConfig: HBaseJournalConfig

  private lazy val replayDispatcherId = journalConfig.replayDispatcherId
  private implicit lazy val replayDispatcher = context.system.dispatchers.lookup(replayDispatcherId)

  import Columns._
  import collection.JavaConverters._

  // todo can be improved to to N parallel scans for each "partition" we created, instead of one "big scan"
  override def asyncReplayMessages(processorId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)
                                  (replayCallback: PersistentRepr => Unit): Future[Unit] = {
    log.debug(s"Async replay for processorId [$processorId], from sequenceNr: [$fromSequenceNr], to sequenceNr: [$toSequenceNr]")

    val scanner = newScanner()
    scanner.setStartKey(RowKey(processorId, fromSequenceNr).toBytes)
    scanner.setStopKey(RowKey(processorId, toSequenceNr).toBytes)
    scanner.setKeyRegexp(RowKey.patternForProcessor(processorId))

    scanner.setMaxNumRows(journalConfig.scanBatchSize)

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
          math.max(reachedSeqNr, seqNrs.max)
        }
    }

    def go() = scanner.nextRows() flatMap handleRows

    go()
  }

  // todo make this multiple scans, on each partition instead of one big scan
  override def asyncReadHighestSequenceNr(processorId: String, fromSequenceNr: Long): Future[Long] = {
    log.debug(s"Async read for highest sequence number for processorId: [$processorId] (hint, seek from  nr: [$fromSequenceNr])")

    val scanner = newScanner()
    scanner.setStartKey(RowKey(processorId, fromSequenceNr).toBytes)
    scanner.setKeyRegexp(RowKey.patternForProcessor(processorId))

    def handleRows(in: AnyRef): Future[Long] = in match {
      case null =>
        log.debug("read highest sequence number finished")
        scanner.close()
        Future(0)

      case rows: JArrayList[JArrayList[KeyValue]] =>
        log.debug(s"asyncReadHighestSequenceNr - got ${rows.size} rows...")
        
        val maxSoFar = rows.asScala.map(cols => sequenceNr(cols.asScala)).max
          
        go() map { reachedSeqNr =>
          math.max(reachedSeqNr, maxSoFar)
        }
    }

    def go() = scanner.nextRows() flatMap handleRows

    go()
  }

  private def replay(replayCallback: (PersistentRepr) => Unit)(columns: mutable.Buffer[KeyValue]): Long = {
    val messageKeyValue = findColumn(columns, Message)
    var msg = persistentFromBytes(messageKeyValue.value)

    val markerKeyValue = findColumn(columns, Marker)
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
  
  private def sequenceNr(columns: mutable.Buffer[KeyValue]): Long = {
    val messageKeyValue = findColumn(columns, Message)
    val msg = persistentFromBytes(messageKeyValue.value)
    msg.sequenceNr
  }

  import Columns._
  private def findColumn(columns: mutable.Buffer[KeyValue], qualifier: Array[Byte]) =
    columns find { kv =>
      ju.Arrays.equals(kv.qualifier, qualifier)
    } getOrElse {
      throw new RuntimeException(s"Unable to find [${Bytes.toString(qualifier)}}] field from: ${columns.map(kv => Bytes.toString(kv.qualifier))}")
    }

  private def newScanner() = {
    val scanner = client.newScanner(Table)
    scanner.setFamily(Family)
    scanner
  }

}
