package akka.persistence.journal.hbase

import akka.persistence.journal.AsyncWriteJournal
import scala.collection.immutable.Seq
import akka.persistence.PersistentRepr
import scala.concurrent._
import scala.concurrent.duration._
import akka.actor.ActorLogging
import org.hbase.async.{HBaseClient => AsyncBaseClient, KeyValue, DeleteRequest, PutRequest}
import org.apache.hadoop.hbase.util.Bytes
import com.stumbleupon.async.Callback
import java.{util => ju}
import java.util.concurrent.atomic.AtomicInteger
import scala.util.Success

/**
 * Asyncronous HBase Journal.
 *
 * Uses AsyncBase to implement asynchronous IPC with HBase.
 */
class HBaseAsyncWriteJournal extends AsyncWriteJournal with HBaseJournalBase
  with HBaseAsyncReplay with PersistenceMarkers
  with DeferredConversions
  with ActorLogging {

  import context.dispatcher

  import Bytes._
  import Columns._
  import collection.JavaConverters._

  val initTimeout = config.getInt("init.timeout").seconds
  val zookeeperQuorum = config.getString("hbase.zookeeper.quorum")

  val client = HBaseAsyncWriteJournal.getClient(zookeeperQuorum)

  override def writeAsync(persistentBatch: Seq[PersistentRepr]): Future[Unit] = {
    log.debug(s"Write async for ${persistentBatch.size} presistent messages")

    val futures = persistentBatch map { p =>
      import p._
      
      executePut(
        RowKey(processorId, sequenceNr).toBytes,
        Array(ProcessorId,          SequenceNr,          Marker,                  Message),
        Array(toBytes(processorId), toBytes(sequenceNr), toBytes(AcceptedMarker), persistentToBytes(p))
      )
    }
    
    Future.sequence(futures)
  }

  override def deleteAsync(processorId: String, fromSequenceNr: Long, toSequenceNr: Long, permanent: Boolean): Future[Unit] = {
    log.debug(s"Delete async for processorId:$processorId from sequenceNr $fromSequenceNr to $toSequenceNr, premanent: $permanent")

    val doDelete =
      if (permanent) deleteRow _
      else markRowAsDeleted _

    val scanner = client.newScanner(TableBytes)
    scanner.setFamily(Family)
    scanner.setQualifier(Marker)
    
    scanner.setStartKey(RowKey(processorId, fromSequenceNr).toBytes)
    scanner.setStopKey(RowKey(processorId, toSequenceNr).toBytes)
    scanner.setMaxNumRows(journalConfig.scanBatchSize)

    def handleRows(in: ju.ArrayList[ju.ArrayList[KeyValue]]): Future[Unit] = in match {
      case null =>
        log.debug(s"Finished scanning (for processorId:$processorId from sequenceNr $fromSequenceNr to $toSequenceNr) in preparation for deletes.")
        scanner.close()

        Future.successful()

      case rows =>
        val deletions = for {
          row <- rows.asScala
          col <- row.asScala
        } yield doDelete(col.key)

        Future.sequence(go() :: deletions.toList)
    }

    def go() = scanner.nextRows() flatMap handleRows

    go()
  }

  override def confirmAsync(processorId: String, sequenceNr: Long, channelId: String): Future[Unit] = {
    log.debug(s"Confirming async for processorId: $processorId, sequenceNr: $sequenceNr and channelId: $channelId")

    executePut(
      RowKey(processorId, sequenceNr).toBytes,
      Array(Marker),
      Array(confirmedMarkerBytes(channelId))
    )
  }

  protected def deleteRow(key: Array[Byte]): Future[Unit] = {
    log.debug(s"Permanently deleting row: ${Bytes.toString(key)}")
    executeDelete(key)
  }

  protected def markRowAsDeleted(key: Array[Byte]): Future[Unit] = {
    log.debug(s"Marking as deleted, for row: ${Bytes.toString(key)}")
    executePut(key, Array(Marker), Array(DeletedMarkerBytes))
  }

  protected def executeDelete(key: Array[Byte]): Future[Unit] = {
    val request = new DeleteRequest(TableBytes, key)
    client.delete(request)
  }
  
  protected def executePut(key: Array[Byte], qualifiers: Array[Array[Byte]], values: Array[Array[Byte]]): Future[Unit] = {
    val request = new PutRequest(TableBytes, key, Family, qualifiers, values)
    client.put(request)
  }

  /**
   * Scans the `.META.` collection in order to check how many regions a table has.
   * Faster than checking explicitly on the table.
   *
   * Could be used to adjust our partition size if user gave us a range for example.
   */
  private def countRegions(tableName: String): Future[Int] = {
    log.info(s"Counting regions for table [$tableName]...")
    val start = System.currentTimeMillis()

    val scanner = client.newScanner(".META.")
    scanner.setFamily("region")
    scanner.setQualifier("region:regioninfo")
    scanner.setKeyRegexp(s"""$tableName,.*""")

    val acc = new AtomicInteger(0)
    val sum = Promise[Int]()

    val act = new Callback[AnyRef, ju.ArrayList[ju.ArrayList[KeyValue]]] {
      def call(arg: ju.ArrayList[ju.ArrayList[KeyValue]]): AnyRef = arg match {
        case null =>
          scanner.close()
          sum.success(acc.get)

        case rows =>
          acc addAndGet rows.size
          rows
      }
    }
    scanner.nextRows().addCallback(act)

    val f = sum.future
    f onComplete { n =>
      val stop = System.currentTimeMillis()
      log.info(s"Finished counting regions for table [$tableName] (took ${stop - start} ms): [$n] regions")
    }

    f
  }

  override def postStop(): Unit = {
    client.shutdown()
    super.postStop()
  }
}

object HBaseAsyncWriteJournal {
  private var _zookeeperQuorum: String = _

  /** based on the docs, there should always be only one instance, reused even if we had more tables */
  private lazy val client = new AsyncBaseClient(_zookeeperQuorum)

  def getClient(zookeeperQuorum: String) = {
    _zookeeperQuorum = zookeeperQuorum
    client
  }
}