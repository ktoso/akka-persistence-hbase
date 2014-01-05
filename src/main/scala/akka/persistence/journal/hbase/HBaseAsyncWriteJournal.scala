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
import java.{util => ju, util}
import java.util.concurrent.atomic.AtomicInteger

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

  val client = new AsyncBaseClient(zookeeperQuorum)


  /** Used to avoid writing all data to the same region - see "hot region" problem */
  def partition(snr: Long) = (snr % partitionCount).toInt

  /** Number of regions the used Table is partitioned to. */
  var partitionCount: Int = _

  /** We must determine the number of regions this table uses before we start reading from it. */
  override def preStart() {
    val regionsCount = countRegions(Table)
    partitionCount = Await.result(regionsCount, atMost = initTimeout)
  }

  override def writeAsync(persistentBatch: Seq[PersistentRepr]): Future[Unit] = {
    val futures = persistentBatch map { p =>
      import p._
      
      executePut(
        rowKey(processorId, sequenceNr),
        Array(ProcessorId,          SequenceNr,          Marker,                  Message),
        Array(toBytes(processorId), toBytes(sequenceNr), toBytes(AcceptedMarker), persistentToBytes(p))
      )
    }
    
    Future.sequence(futures)
  }

  // todo most probably racy internally... fix me
  override def deleteAsync(processorId: String, fromSequenceNr: Long, toSequenceNr: Long, permanent: Boolean): Future[Unit] = {
    val deleteCommand =
      if (permanent) deleteRow _
      else markRowAsDeleted _

    val scanner = client.newScanner(TableBytes)
    ???
//    scanner.setStartKey()
//    Future {
//      scan(processorId, fromSequenceNr, toSequenceNr) { res =>
//        val key = res.getRow
//        deleteCommand(key)
//      }
//    } flatMap {
//      Future.sequence(_)
//    }
  }

  override def confirmAsync(processorId: String, sequenceNr: Long, channelId: String): Future[Unit] = {
    executePut(
      rowKey(processorId, sequenceNr),
      Array(Marker),
      Array(confirmedMarkerBytes(channelId))
    )
  }

  private def deleteRow(key: Array[Byte]): Future[Unit] = {
    val p = Promise[Unit]()
    client.delete(new DeleteRequest(TableBytes, key))
      .addCallback(new Callback[AnyRef, AnyRef] {
      def call(arg: AnyRef) = p.complete(null)
    })
    p.future
  }

  protected def executeDelete(key: Array[Byte]): Future[Unit] = {
    val request = new DeleteRequest(TableBytes, key)
    client.delete(request)
  }
  
  protected def executePut(key: Array[Byte], qualifiers: Array[Array[Byte]], values: Array[Array[Byte]]): Future[Unit] = {
    val request = new PutRequest(TableBytes, key, Family, qualifiers, values)
    client.put(request)
  }

  private def markRowAsDeleted(key: Array[Byte]): Future[Unit] =
    executePut(key, Array(Marker), Array(DeletedMarkerBytes))

  /**
   * Scans the `.META.` collection in order to check how many regions a table has.
   * Faster than checking explicitly on the table.
   */
  def countRegions(tableName: String): Future[Int] = {
    log.info(s"Counting regions for table [$tableName]...")
    val start = System.currentTimeMillis()

    val scanner = client.newScanner(".META.")
    scanner.setQualifier("region:regioninfo")
    scanner.setKeyRegexp(s"""$tableName,.*""")

    val acc = new AtomicInteger(0)
    val sum = Promise[Int]()

    scanner.nextRows().addCallback(new Callback[AnyRef, ju.ArrayList[ju.ArrayList[KeyValue]]] {
      def call(arg: util.ArrayList[util.ArrayList[KeyValue]]): AnyRef = arg match {
        case null =>
          scanner.close()
          sum.success(acc.get)

        case rows =>
          acc addAndGet rows.size
          rows
      }
    })

    val f = sum.future
    f onComplete { n =>
      val stop = System.currentTimeMillis()
      log.info(s"Finished counting regions for table [$tableName] (took ${stop - start} ms): [$n] regions")
    }

    f
  }

  override def postStop(): Unit = {
    super.postStop()
    client.shutdown()
  }
}
