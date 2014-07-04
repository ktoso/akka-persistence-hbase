package akka.persistence.hbase.journal

import java.{util => ju}

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.persistence.PersistentRepr
import akka.persistence.hbase.common.RowKey
import akka.persistence.hbase.journal.Resequencer.AllPersistentsSubmitted
import akka.persistence.journal._
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.util.Bytes
import org.hbase.async.{HBaseClient, KeyValue}

import scala.collection.mutable
import scala.concurrent.{Future, Promise}

trait HBaseAsyncRecovery extends AsyncRecovery {
  this: Actor with ActorLogging with HBaseAsyncWriteJournal =>

  private[persistence] def client: HBaseClient

  private[persistence] implicit def hBasePersistenceSettings: PluginPersistenceSettings

  private lazy val replayDispatcherId = hBasePersistenceSettings.replayDispatcherId

  override implicit val pluginDispatcher = context.system.dispatchers.lookup(replayDispatcherId)

  import akka.persistence.hbase.common.Columns._
  import akka.persistence.hbase.common.DeferredConversions._

import scala.collection.JavaConverters._

  // async recovery plugin impl

  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)
                                  (replayCallback: PersistentRepr => Unit): Future[Unit] = {
    log.debug(s"Async replay for persistenceId [$persistenceId], from sequenceNr: [$fromSequenceNr], to sequenceNr: [$toSequenceNr]")

    val reachedSeqNrPromise = Promise[Long]()
    val resequencer = context.actorOf(Resequencer.props(replayCallback, reachedSeqNrPromise, replayDispatcherId))

    val partitions = hBasePersistenceSettings.partitionCount

    def scanPartition(part: Long, resequencer: ActorRef): Long = {
      val startScanKey = RowKey.firstInPartition(persistenceId, part, fromSequenceNr) // 021-ID-0000000000000000021
      val stopScanKey = RowKey.lastInPartition(persistenceId, part, toSequenceNr)     // 021-ID-9223372036854775800
      val persistenceIdRowRegex = RowKey.patternForProcessor(persistenceId)           //  .*-ID-.*

      log.info("Scanning {} partition, from {} to {}", part, startScanKey.toKeyString, stopScanKey.toKeyString)

      val scan = new Scan
      scan.setStartRow(startScanKey.toBytes) // inclusive
      scan.setStopRow(stopScanKey.toBytes) // exclusive
      scan.setBatch(hBasePersistenceSettings.scanBatchSize)

      scan.addColumn(FamilyBytes, Marker)
      scan.addColumn(FamilyBytes, Message)

      scan.setFilter(new RowFilter(CompareOp.EQUAL, new RegexStringComparator(persistenceIdRowRegex)))

      val scanner = hTable.getScanner(scan)
      var scheduled = 0L
      try {
        var res = scanner.next()
        while (res != null) {

          val markerCell = res.getColumnLatestCell(FamilyBytes, Marker)
          val messageCell = res.getColumnLatestCell(FamilyBytes, Message)

          if ((markerCell ne null) && (messageCell ne null)) {
            val marker = Bytes.toString(CellUtil.cloneValue(markerCell))

            marker match {
              case "A" =>
                val persistentRepr = persistentFromBytes(CellUtil.cloneValue(messageCell))

                val seqNr = persistentRepr.sequenceNr
                if (fromSequenceNr <= seqNr && seqNr <= toSequenceNr) {
                  resequencer ! persistentRepr
                  scheduled += 1
                }

              case "S" =>
                // thanks to treating Snapshot rows as deleted entries, we won't suddenly apply a Snapshot() where the
                // our Processor expects a normal message. This is implemented for the HBase backed snapshot storage,
                // if you use the HDFS storage there won't be any snapshot entries in here.
                // As for message deletes: if we delete msgs up to seqNr 4, and snapshot was at 3, we want to delete it anyway.
                // treat as deleted, ignore...

              case "D" =>
                // marked as deleted, ignore...

              case _ =>
                // channel confirmation
                val persistentRepr = persistentFromBytes(CellUtil.cloneValue(messageCell))

                val channelId = RowTypeMarkers.extractSeqNrFromConfirmedMarker(marker)
                replayCallback(persistentRepr.update(confirms = channelId +: persistentRepr.confirms))
            }
          }
          res = scanner.next()
        }
        scheduled
      } finally {
        log.debug("Done scheduling replays in partition {} (scheduled: {})", part, scheduled)
        scanner.close()

        scheduled
      }
    }

    val partitionScans = (1 to partitions).map(i => Future { scanPartition(i, resequencer) })
    Future.sequence(partitionScans) onComplete { _ => resequencer ! AllPersistentsSubmitted }

    reachedSeqNrPromise.future map { case _ =>
      log.info("Completed recovery scanning for persistenceId {}", persistenceId)
    }
  }

  // todo make this multiple scans, on each partition instead of one big scan
  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    log.debug(s"Async read for highest sequence number for persistenceId: [$persistenceId] (hint, seek from  nr: [$fromSequenceNr])")

    val scanner = newScanner()
    scanner.setStartKey(RowKey(selectPartition(fromSequenceNr), persistenceId, fromSequenceNr).toBytes)
    scanner.setStartKey(RowKey.lastForPersistenceId(persistenceId).toBytes)
    scanner.setKeyRegexp(RowKey.patternForProcessor(persistenceId))

    def handleRows(in: AnyRef): Future[Long] = in match {
      case null =>
        scanner.close()
        Future(0)

      case rows: AsyncBaseRows =>
        log.debug(s"AsyncReadHighestSequenceNr - got ${rows.size} rows...")
        
        val maxSoFar = rows.asScala.map(cols => sequenceNr(cols.asScala)).max
          
        go() map { reachedSeqNr =>
          math.max(reachedSeqNr, maxSoFar)
        }
    }

    def go() = scanner.nextRows(hBasePersistenceSettings.scanBatchSize) flatMap handleRows

    go() map { case l =>
      log.debug("Finished scanning for highest sequence number: {}", l)
      l
    }
  }


//  end of async recovery plugin impl

  private def sequenceNr(columns: mutable.Buffer[KeyValue]): Long = {
    val messageKeyValue = findColumn(columns, Message)
    val msg = persistentFromBytes(messageKeyValue.value)
    msg.sequenceNr
  }

}

/**
 * This is required because of the way we store messages in the HTable (prefixed with a seed, in order to avoid the "hot-region problem").
 *
 * Note: The hot-region problem is when a lot of traffic goes to exactly one region, while the other regions "do nothing".
 *       This problem happens esp. with sequential numbering - such as the sequenceNr. The prefix-seeding solves this problem
 *       but it introduces out-of-sequence order scanning (a scan will read 000-a-05 before 001-a-01), which is wy the [[Resequencer]] is needed.
 *
 * @param replayCallback the callback which we want to call with sequenceNr ascending-order messages
 */
private[hbase] class Resequencer(replayCallback: PersistentRepr => Unit, reachedSeqNr: Promise[Long]) extends Actor with ActorLogging {

  private var allSubmitted = false

  private val delayed = mutable.Map.empty[Long, PersistentRepr]
  private var delivered = 0L

  import akka.persistence.hbase.journal.Resequencer._

  def receive = {
    case d: PersistentRepr ⇒ resequence(d)
    case AllPersistentsSubmitted =>
      if (delayed.isEmpty) completeResequencing()
      else allSubmitted = true
  }

  @scala.annotation.tailrec
  private def resequence(p: PersistentRepr) {
    if (p.sequenceNr == delivered + 1) {
      delivered = p.sequenceNr
      replayCallback(p)

      if (allSubmitted && delayed.isEmpty)
        completeResequencing()
    } else {
      delayed += (p.sequenceNr -> p)
    }

    val ro = delayed.remove(delivered + 1)
    if (ro.isDefined) resequence(ro.get)
  }

  private def completeResequencing() {
    log.debug("All messages have been resequenced and applied (until seqNr: {})!", delivered)
    reachedSeqNr success delivered
    context stop self
  }
}

private[hbase] object Resequencer {
  def props(replayCallback: PersistentRepr => Unit, reachedSeqNr: Promise[Long], dispatcherId: String) =
    Props(classOf[Resequencer], replayCallback, reachedSeqNr).withDispatcher(dispatcherId) // todo stop it at some point

  case object AllPersistentsSubmitted
}