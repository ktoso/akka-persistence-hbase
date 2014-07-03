package akka.persistence.hbase.journal

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.persistence.hbase.common._
import akka.persistence.hbase.journal.Operator.AllOpsSubmitted
import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.{PersistenceSettings, PersistentConfirmation, PersistentId, PersistentRepr}
import com.google.common.base.Stopwatch
import org.apache.hadoop.hbase.client.{HTable, Scan}
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter._

import scala.collection.immutable
import scala.concurrent._

/**
 * Asyncronous HBase Journal.
 *
 * Uses AsyncBase to implement asynchronous IPC with HBase.
 */
class HBaseAsyncWriteJournal extends Actor with ActorLogging
  with HBaseJournalBase with AsyncWriteJournal
  with HBaseAsyncRecovery {

  import akka.persistence.hbase.common.TestingEventProtocol._
  import akka.persistence.hbase.journal.RowTypeMarkers._

  private lazy val config = context.system.settings.config

  implicit lazy val hBasePersistenceSettings = PluginPersistenceSettings(config)

  lazy val hadoopConfig = HBaseJournalInit.getHBaseConfig(config)

  lazy val client = HBaseClientFactory.getClient(hBasePersistenceSettings, new PersistenceSettings(config.getConfig("akka.persistence")))

  lazy val publishTestingEvents = hBasePersistenceSettings.publishTestingEvents

  implicit override val pluginDispatcher = context.system.dispatchers.lookup(hBasePersistenceSettings.pluginDispatcherId)


  import akka.persistence.hbase.common.Columns._
  import akka.persistence.hbase.common.DeferredConversions._
  import org.apache.hadoop.hbase.util.Bytes._

  // journal plugin api impl -------------------------------------------------------------------------------------------

  override def asyncWriteMessages(persistentBatch: immutable.Seq[PersistentRepr]): Future[Unit] = {
    log.debug(s"Write async for {} presistent messages", persistentBatch.size)
    val watch = (new Stopwatch).start()

    val futures = persistentBatch map { p =>
      import p._
      
      executePut(
        RowKey(persistenceId, sequenceNr).toBytes,
        Array(PersistenceId,          SequenceNr,          Marker,                  Message),
        Array(toBytes(persistenceId), toBytes(sequenceNr), toBytes(AcceptedMarker), persistentToBytes(p))
      )
    }

    flushWrites()
    Future.sequence(futures) map { case _ =>
      log.debug("Completed writing {} messages (took: {})", persistentBatch.size, watch.stop()) // todo better failure / success?
      if (publishTestingEvents) context.system.eventStream.publish(FinishedWrites(persistentBatch.size))
    }
  }

  // todo should be optimised to do ranged deletes
  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long, permanent: Boolean): Future[Unit] = {
    val watch = (new Stopwatch).start()
    log.debug(s"AsyncDeleteMessagesTo for persistenceId: {} to sequenceNr: {}, premanent: {}", persistenceId, toSequenceNr, permanent)

    // prepare delete function (delete or mark as deleted)
    val doDelete = deleteFunctionFor(permanent)

    def enqueueDeleteOps(operator: ActorRef): Unit = {
      val startScanKey = RowKey.firstForProcessor(persistenceId)                     // 000-ID-000000000000
      val stopScanKey = RowKey.lastForProcessorScan(persistenceId, toSequenceNr + 1) // 999-ID-0000[seqNr]
      val persistenceIdRowRegex = RowKey.patternForProcessor(persistenceId)          //  .*-ID-.*

      val scan = new Scan
      scan.setStartRow(startScanKey.toBytes)
      scan.setStopRow(stopScanKey.toBytes)

      val fl = new FilterList()
      fl.addFilter(new RowFilter(CompareOp.EQUAL, new RegexStringComparator(persistenceIdRowRegex)))
      fl.addFilter(new FirstKeyOnlyFilter)
      fl.addFilter(new KeyOnlyFilter)
      scan.setFilter(fl)

      log.debug("Scanning for keys to delete, start: {}, stop: {}, regex: {}", startScanKey, stopScanKey, persistenceIdRowRegex)

      val table = new HTable(hadoopConfig, Table)
      try {
        val scanner = table.getScanner(scan)

        try {
          var res = scanner.next()
          while (res != null) {
            operator ! res.getRow
            res = scanner.next()
          }
        } finally {
          operator ! AllOpsSubmitted
          scanner.close()
        }
      } finally table.close()
    }

    val deleteRowsPromise = Promise[Unit]()

    val operator = context.actorOf(Operator.props(deleteRowsPromise, doDelete, hBasePersistenceSettings.pluginDispatcherId))
    Future { enqueueDeleteOps(operator) }

    deleteRowsPromise.future map { case _ =>
      log.debug("Finished deleting messages for persistenceId: {}, to sequenceNr: {}, permanent: {} (took: {})", persistenceId, toSequenceNr, permanent, watch.stop())
      if (publishTestingEvents) context.system.eventStream.publish(FinishedDeletes(toSequenceNr))
    }
  }

  @deprecated("Will be removed")
  override def asyncWriteConfirmations(confirmations: immutable.Seq[PersistentConfirmation]): Future[Unit] = {
    log.debug(s"AsyncWriteConfirmations for {} messages", confirmations.size)
    val watch = (new Stopwatch).start()

    val fs = confirmations map { confirm =>
      confirmAsync(confirm.persistenceId, confirm.sequenceNr, confirm.channelId)
    }

    flushWrites()
    Future.sequence(fs) map { case _ =>
      log.debug("Completed confirming {} messages (took: {})", confirmations.size, watch.stop()) // todo better failure / success?
    }
  }

  @deprecated("Will be removed")
  override def asyncDeleteMessages(messageIds: immutable.Seq[PersistentId], permanent: Boolean): Future[Unit] = {
    log.debug(s"Async delete [{}] messages, premanent: {}", messageIds.size, permanent)

    val doDelete = deleteFunctionFor(permanent)

    val deleteFutures = for {
      messageId <- messageIds
      rowId = RowKey(messageId.persistenceId, messageId.sequenceNr)
    } yield doDelete(rowId.toBytes)

    flushWrites()
    Future.sequence(deleteFutures)
  }

  // end of journal plugin api impl ------------------------------------------------------------------------------------

  private def confirmAsync(persistenceId: String, sequenceNr: Long, channelId: String): Future[Unit] = {
      log.debug(s"Confirming async for persistenceId: {}, sequenceNr: {} and channelId: {}", persistenceId, sequenceNr, channelId)

      executePut(
        RowKey(persistenceId, sequenceNr).toBytes,
        Array(Marker),
        Array(confirmedMarkerBytes(channelId))
      )
    }

  private def deleteFunctionFor(permanent: Boolean): (Array[Byte]) => Future[Unit] = {
    if (permanent) deleteRow
    else markRowAsDeleted
  }

  override def postStop(): Unit = {
    client.shutdown()
    super.postStop()
  }
}

/**
 * Actor which gets row keys and performs operations on them.
 * Completes the given `finish` promoise once all keys have been processed.
 *
 * Requires being notified when there's no more incoming work, by sending [[Operator.AllOpsSubmitted]]
 *
 * @param finish promise to complete one all ops have been applied to the submitted keys
 * @param op operation to be applied on each submitted key
 */
private[hbase] class Operator(finish: Promise[Unit], op: Array[Byte] => Future[Unit]) extends Actor with ActorLogging {

  var totalOps: Long = 0 // how many ops were we given to process (from user-land)
  var processedOps: Long = 0 // how many ops are pending to finish (from hbase-land)

  var allOpsSubmitted = false // are we don submitting ops to be applied?

  import akka.persistence.hbase.journal.Operator._
  import context.dispatcher

  def receive = {
    case key: Array[Byte] =>
      totalOps += 1
      op(key) foreach { _ => self ! OpApplied(key) }

    case AllOpsSubmitted =>
      log.debug("Received a total of {} ops to execute.", totalOps)
      allOpsSubmitted = true

    case OpApplied(key) =>
      processedOps += 1

      if (allOpsSubmitted && (processedOps == totalOps)) {
        log.debug("Finished processing all {} ops.", totalOps)
        finish.success(())
        context stop self
      }
  }
}
object Operator {

  def props(deleteRowsPromise: Promise[Unit], doDelete: Array[Byte] => Future[Unit], dispatcher: String): Props =
    Props(classOf[Operator], deleteRowsPromise, doDelete).withDispatcher(dispatcher)

  final case class OpApplied(row: Array[Byte])
  object AllOpsSubmitted
}