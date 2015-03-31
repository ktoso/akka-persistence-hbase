package akka.persistence.hbase.journal

import java.io.IOException
import java.util.concurrent.atomic.AtomicBoolean

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.persistence.PersistentRepr
import akka.persistence.hbase.common.RowKey
import akka.persistence.hbase.journal.Resequencer.AllPersistentsSubmitted
import akka.persistence.journal._
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.util.Bytes
import org.hbase.async.{HBaseClient, KeyValue}

import scala.collection.mutable
import scala.concurrent.{Future, Promise}

trait HBaseAsyncRecovery extends AsyncRecovery {
  this: Actor with ActorLogging with HBaseAsyncWriteJournal =>

  private[persistence] def client: HBaseClient

  private[persistence] implicit def hBasePersistenceSettings: PersistencePluginSettings

  private lazy val replayDispatcherId = hBasePersistenceSettings.replayDispatcherId

  override implicit val pluginDispatcher = context.system.dispatchers.lookup(replayDispatcherId)

  import akka.persistence.hbase.common.Columns._

  // async recovery plugin impl

  override def asyncReplayMessages(persistenceId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)
                                  (replayCallback: PersistentRepr => Unit): Future[Unit] = max match {
    case 0 =>
      log.debug("Skipping async replay for persistenceId [{}], from sequenceNr: [{}], to sequenceNr: [{}], since max messages count to replay is 0",
        persistenceId, fromSequenceNr, toSequenceNr)

      Future.successful() // no need to do a replay anything

    case _ =>
      log.debug("Async replay for persistenceId [{}], from sequenceNr: [{}], to sequenceNr: [{}]{}",
        persistenceId, fromSequenceNr, toSequenceNr, if (max != Long.MaxValue) s", limited to: $max messages" else "")

      val reachedSeqNrPromise = Promise[Long]()
      val loopedMaxFlag = new AtomicBoolean(false) // the resequencer may let us know that it looped `max` messages, and we can abort further scanning
      val resequencer = context.actorOf(Resequencer.props(fromSequenceNr, max, replayCallback, loopedMaxFlag, reachedSeqNrPromise, replayDispatcherId))

      val partitions = hBasePersistenceSettings.partitionCount

      def scanPartition(part: Long, resequencer: ActorRef): Long = {
        val startScanKey = RowKey.firstInPartition(persistenceId, part, fromSequenceNr) // 021-ID-0000000000000000021
        val stopSequenceNr = if (toSequenceNr < Long.MaxValue) toSequenceNr + 1 else Long.MaxValue
        val stopScanKey = RowKey.lastInPartition(persistenceId, part, stopSequenceNr) // 021-ID-9223372036854775800
        val persistenceIdRowRegex = RowKey.patternForProcessor(persistenceId) //  .*-ID-.*

        // we can avoid canning some partitions - guaranteed to be empty for smaller than the partition number seqNrs
        if (part > toSequenceNr)
          return 0

        log.debug("Scanning {} partition for replay, from {} to {}", part, startScanKey.toKeyString, stopScanKey.toKeyString)

        val scan = preparePartitionScan(tableBytes, familyBytes, startScanKey, stopScanKey, persistenceIdRowRegex, onlyRowKeys = false)
        val scanner = hTable.getScanner(scan)

        var lowestSeqNr: Long = 0L

        def resequenceMsg(persistentRepr: PersistentRepr) {
          val seqNr = persistentRepr.sequenceNr
          if (fromSequenceNr <= seqNr && seqNr <= toSequenceNr) {
            resequencer ! persistentRepr
            lowestSeqNr = seqNr
          }
        }

        try {
          var res = scanner.next()
          while (res != null) {
            // Note: In case you wonder why we can't break the loop with a simple counter here once we loop through `max` elements:
            // Since this is multiple scans, on multiple partitions, they are not ordered, yet we must deliver ordered messages
            // to the receiver. Only the resequencer knows how many are really "delivered"

            val markerCell = res.getColumnLatestCell(familyBytes, Marker)
            val messageCell = res.getColumnLatestCell(familyBytes, Message)

            if ((markerCell ne null) && (messageCell ne null)) {
              val marker = Bytes.toString(CellUtil.cloneValue(markerCell))

              marker match {
                case "A" =>
                  val persistentRepr = persistentFromBytes(CellUtil.cloneValue(messageCell))
                  resequenceMsg(persistentRepr)

                case "S" =>
                  // thanks to treating Snapshot rows as deleted entries, we won't suddenly apply a Snapshot() where the
                  // our Processor expects a normal message. This is implemented for the HBase backed snapshot storage,
                  // if you use the HDFS storage there won't be any snapshot entries in here.
                  // As for message deletes: if we delete msgs up to seqNr 4, and snapshot was at 3, we want to delete it anyway.


                case "D" =>
                  // mark as deleted, journal may choose to replay it
                  val persistentRepr = persistentFromBytes(CellUtil.cloneValue(messageCell))
                  resequenceMsg(persistentRepr.update(deleted = true))

                case _ =>
                  // channel confirmation
                  val persistentRepr = persistentFromBytes(CellUtil.cloneValue(messageCell))

                  val channelId = RowTypeMarkers.extractSeqNrFromConfirmedMarker(marker)
                  replayCallback(persistentRepr.update(confirms = channelId +: persistentRepr.confirms))
              }
            }
            res = scanner.next()
          }
          lowestSeqNr
        } finally {
          if (lowestSeqNr > 0) log.debug("Done scheduling replays in partition {} (lowest seqNr: {})", part, lowestSeqNr)
          scanner.close()
        }
      }

      val partitionScans = (1 to partitions).map(i => Future { scanPartition(i, resequencer) })
      Future.sequence(partitionScans) onSuccess {
        case lowestSeqNrInEachPartition =>
          val seqNrs = lowestSeqNrInEachPartition.filterNot(_ == 0L).toList

          if (seqNrs.nonEmpty)
            resequencer ! AllPersistentsSubmitted(assumeSequenceStartsAt = seqNrs.min)
          else
            resequencer ! AllPersistentsSubmitted(assumeSequenceStartsAt = 0)
      }

      reachedSeqNrPromise.future map { case _ =>
        log.debug("Completed recovery scanning for persistenceId {}", persistenceId)
      }
  }

  // todo make this multiple scans, on each partition instead of one big scan
  override def asyncReadHighestSequenceNr(persistenceId: String, fromSequenceNr: Long): Future[Long] = {
    log.debug(s"Async read for highest sequence number for persistenceId: [$persistenceId] (hint, seek from  nr: [$fromSequenceNr])")

    val partitions = hBasePersistenceSettings.partitionCount

    def scanPartitionForMaxSeqNr(part: Long): Long = {
      val startScanKey = RowKey.firstInPartition(persistenceId, part, fromSequenceNr) // 021-ID-0000000000000000021
      val toSequenceNr = RowKey.lastSeqNrInPartition(part)
      val stopSequenceNr = if (toSequenceNr < Long.MaxValue) toSequenceNr + 1 else Long.MaxValue
      val stopScanKey = RowKey.lastInPartition(persistenceId, part, stopSequenceNr)   // 021-ID-9223372036854775897
      val persistenceIdRowRegex = RowKey.patternForProcessor(persistenceId)           //  .*-ID-.*

//      log.debug("Scanning {} partition, from {} to {}", part, startScanKey.toKeyString, stopScanKey.toKeyString)

      val scan = preparePartitionScan(tableBytes, familyBytes, startScanKey, stopScanKey, persistenceIdRowRegex, onlyRowKeys = true)
      val scanner = hTable.getScanner(scan)

      var highestSeqNr: Long = 0L

      try {
        var res = scanner.next()
        while (res != null) {
          val seqNr = RowKey.extractSeqNr(res.getRow)
          highestSeqNr = math.max(highestSeqNr, seqNr)
          res = scanner.next()
        }
        highestSeqNr
      } finally {
        if (highestSeqNr > 0) log.debug("Done scheduling replays in partition {} (highest seqNr: {})", part, highestSeqNr)
        scanner.close()
      }
    }

    val partitionScans = (1 to partitions).map(i => Future { scanPartitionForMaxSeqNr(i) })
    Future.sequence(partitionScans) map {
      case seqNrs if seqNrs.isEmpty => 0L
      case seqNrs => seqNrs.max
    } map { seqNr =>
      log.debug("Found highest seqNr for persistenceId: {}, it's: {}", persistenceId, seqNr)
      seqNr
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
 * @param maxMsgsToSequence max number of messages to be resequenced, usualy Long.MaxValue, but can be used to perform partial replays
 * @param loopedMaxFlag switched to `true` once `maxMsgsToSequence` is reached, with the goal of shortcircutting scanning the HTable
 * @param sequenceStartsAt since we support partial replays (from 4 to 100), the resequencer must know when to start replaying
 */
private[hbase] class Resequencer(
    private var sequenceStartsAt: Long,
    maxMsgsToSequence: Long,
    replayCallback: PersistentRepr => Unit,
    loopedMaxFlag: AtomicBoolean,
    reachedSeqNr: Promise[Long]
  ) extends Actor with ActorLogging {

  private var allSubmitted = false

  private val delayed = mutable.Map.empty[Long, PersistentRepr]
  private var deliveredSeqNr = sequenceStartsAt - 1
  private def deliveredMsgs = deliveredSeqNr - sequenceStartsAt + 1

  import akka.persistence.hbase.journal.Resequencer._

  def receive = {
    case p: PersistentRepr ⇒
//      log.debug("Resequencing {} from {}; Delivered until {} already", p.payload, p.sequenceNr, deliveredSeqNr)
      resequence(p)

    case AllPersistentsSubmitted(assumeSequenceStartsAt) =>
      if (deliveredMsgs == 0L) {
        // kick off recovery from the assumed lowest seqNr
        // could be not 1 because of permanent deletion, yet replay was requested from 1
        this.sequenceStartsAt = assumeSequenceStartsAt
        this.deliveredSeqNr = sequenceStartsAt - 1

        val ro = delayed.remove(deliveredSeqNr + 1)
        if (ro.isDefined) resequence(ro.get)
      }

      if (delayed.isEmpty) completeResequencing()
      else {
        allSubmitted = true
        failResequencing()
      }
  }

  @scala.annotation.tailrec
  private def resequence(p: PersistentRepr) {

    if (p.sequenceNr == deliveredSeqNr + 1) {
      deliveredSeqNr = p.sequenceNr
//      log.debug("Applying {} @ {}", p.payload, p.sequenceNr)
      replayCallback(p)

      if (deliveredMsgs == maxMsgsToSequence) {
        delayed.clear()
        loopedMaxFlag set true

        completeResequencing()
      } else if (allSubmitted && delayed.isEmpty) {
        completeResequencing()
      }
    } else {
      delayed += (p.sequenceNr -> p)
    }

    val ro = delayed.remove(deliveredSeqNr + 1)
    if (ro.isDefined) resequence(ro.get)
  }

  private def completeResequencing() {
    log.debug("All messages have been resequenced and applied (until seqNr: {}, nr of messages: {})!", deliveredSeqNr, deliveredMsgs)
    reachedSeqNr success deliveredSeqNr
    context stop self
  }
  private def failResequencing() {
    log.error("All persistents supbmitted but delayed is not empty, some messages must fail to replay.")
    reachedSeqNr failure new IOException("Failed to complete resequence replay msgs.")
    context stop self
  }
}

private[hbase] object Resequencer {

  def props(sequenceStartsAt: Long, maxMsgsToSequence: Long, replayCallback: PersistentRepr => Unit, loopedMaxFlag: AtomicBoolean, reachedSeqNr: Promise[Long], dispatcherId: String) =
    Props(classOf[Resequencer], sequenceStartsAt, maxMsgsToSequence, replayCallback, loopedMaxFlag, reachedSeqNr).withDispatcher(dispatcherId) // todo stop it at some point

  final case class AllPersistentsSubmitted(assumeSequenceStartsAt: Long)
}