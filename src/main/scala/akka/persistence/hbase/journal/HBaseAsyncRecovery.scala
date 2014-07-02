package akka.persistence.hbase.journal

import java.{util => ju}

import akka.actor.{Actor, ActorLogging}
import akka.persistence.PersistentRepr
import akka.persistence.hbase.common.RowKey
import akka.persistence.journal._
import org.apache.hadoop.hbase.util.Bytes
import org.hbase.async.{HBaseClient, KeyValue}

import scala.annotation.switch
import scala.collection.mutable
import scala.concurrent.Future

trait HBaseAsyncRecovery extends AsyncRecovery {
  this: Actor with ActorLogging with HBaseAsyncWriteJournal =>

  private[persistence] def client: HBaseClient

  private[persistence] implicit def hBasePersistenceSettings: PluginPersistenceSettings

  private lazy val replayDispatcherId = hBasePersistenceSettings.replayDispatcherId

  override implicit val executionContext = context.system.dispatchers.lookup(replayDispatcherId)

  import akka.persistence.hbase.common.Columns._
  import akka.persistence.hbase.common.DeferredConversions._
  import akka.persistence.hbase.journal.RowTypeMarkers._

import scala.collection.JavaConverters._

  // async recovery plugin impl

  // todo can be improved to to N parallel scans for each "partition" we created, instead of one "big scan"
  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)
                                  (replayCallback: PersistentRepr => Unit): Future[Unit] = {
    log.debug(s"Async replay for persistenceId [$persistenceId], from sequenceNr: [$fromSequenceNr], to sequenceNr: [$toSequenceNr]")

    val scanner = newScanner()
    scanner.setStartKey(RowKey(persistenceId, fromSequenceNr).toBytes)
    scanner.setStopKey(RowKey(persistenceId, toSequenceNr).toBytes)
    scanner.setKeyRegexp(RowKey.patternForProcessor(persistenceId))

    scanner.setMaxNumRows(hBasePersistenceSettings.scanBatchSize)

    val callback = replay(replayCallback) _

    def handleRows(in: AnyRef): Future[Long] = in match {
      case null =>
        log.debug("replayAsync - finished!")
        scanner.close()
        Future(0L)

      case rows: AsyncBaseRows =>
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
  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    log.debug(s"Async read for highest sequence number for persistenceId: [$persistenceId] (hint, seek from  nr: [$fromSequenceNr])")

    val scanner = newScanner()
    scanner.setStartKey(RowKey(persistenceId, fromSequenceNr).toBytes)
    scanner.setKeyRegexp(RowKey.patternForProcessor(persistenceId))

    def handleRows(in: AnyRef): Future[Long] = in match {
      case null =>
        log.debug("AsyncReadHighestSequenceNr finished")
        scanner.close()
        Future(0)

      case rows: AsyncBaseRows =>
        log.debug(s"AsyncReadHighestSequenceNr - got ${rows.size} rows...")
        
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

    // todo make this a @switch
    (markerKeyValue.value.head.toChar: @switch) match {
      case 'A' =>
        replayCallback(msg)

      case 'D' =>
        msg = msg.update(deleted = true)

      case 'S' =>
        // thanks to treating Snapshot rows as deleted entries, we won't suddenly apply a Snapshot() where the
        // our Processor expects a normal message. This is implemented for the HBase backed snapshot storage,
        // if you use the HDFS storage there won't be any snapshot entries in here.
        // As for message deletes: if we delete msgs up to seqNr 4, and snapshot was at 3, we want to delete it anyway.
        msg = msg.update(deleted = true)

      case _ =>
        val channelId = extractSeqNrFromConfirmedMarker(marker)
        msg = msg.update(confirms = channelId +: msg.confirms)
        replayCallback(msg)
    }

    msg.sequenceNr
  }

  // end of async recovery plugin impl

  private def sequenceNr(columns: mutable.Buffer[KeyValue]): Long = {
    val messageKeyValue = findColumn(columns, Message)
    val msg = persistentFromBytes(messageKeyValue.value)
    msg.sequenceNr
  }

}
