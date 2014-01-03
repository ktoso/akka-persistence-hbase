package akka.persistence.journal.hbase

import akka.persistence.journal.AsyncWriteJournal
import scala.collection.immutable.Seq
import akka.persistence.PersistentRepr
import scala.concurrent._
import akka.actor.ActorLogging

/**
 * Wraps syncronous operations with Futures, so this is NOT a fully asynchronous Journal.
 * Try it our yourself and either use the syncronous [[akka.persistence.journal.hbase.HBaseSyncWriteJournal]] if this impl doesn't cut it.
 *
 * TODO: Investigate if <a href="http://tsunanet.net/~tsuna/asynchbase/api/org/hbase/async/HBaseClient.html">AsyncBase</a> is really asynchronous and possibly implement using it instead.
 */
class HBaseAsyncWriteJournal extends AsyncWriteJournal with HBaseJournalBase
  with ActorLogging
  with HBaseAsyncReplay with PersistenceMarkers {

  // TODO: replace with really async impl. Check Tsuna's AsyncBase

  import context.dispatcher

  /** WARNING: Plain HBase does not provide async APIs, thus this impl. only wraps the syncronous operations in a Future. */
  override def writeAsync(persistentBatch: Seq[PersistentRepr]): Future[Unit] =
    Future { write(persistentBatch) }

  /** WARNING: Plain HBase does not provide async APIs, thus this impl. only wraps the syncronous operations in a Future. */
  override def deleteAsync(processorId: String, fromSequenceNr: Long, toSequenceNr: Long, permanent: Boolean): Future[Unit] =
    Future { delete(processorId, fromSequenceNr, toSequenceNr, permanent) }

  /** WARNING: Plain HBase does not provide async APIs, thus this impl. only wraps the syncronous operations in a Future. */
  override def confirmAsync(processorId: String, sequenceNr: Long, channelId: String): Future[Unit] =
    Future { confirm(processorId, sequenceNr, channelId) }

}
