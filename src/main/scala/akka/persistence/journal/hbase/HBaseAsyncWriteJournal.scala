package akka.persistence.journal.hbase

import akka.persistence.journal.AsyncWriteJournal
import scala.collection.immutable.Seq
import akka.persistence.PersistentRepr
import scala.concurrent._
import akka.actor.ActorLogging
import org.hbase.async.{HBaseClient => AsyncBaseClient, DeleteRequest, KeyValue, PutRequest}
import org.apache.hadoop.hbase.util.Bytes
import com.stumbleupon.async.{Deferred, Callback}
import java.util
import akka.actor.Status.Success

/**
 * Asyncronous HBase Journal.
 *
 * Uses AsyncBase to implement asynchronous IPC with HBase.
 *
 * TODO: Warning, Delete seems racy still.
 */
class HBaseAsyncWriteJournal extends AsyncWriteJournal with HBaseJournalBase
  with ActorLogging
  with HBaseAsyncReplay with PersistenceMarkers {

  import context.dispatcher
  import Bytes._
  import Columns._
  
  val asyncClient = new AsyncBaseClient(config.getString("zookeeper.quorum"))

  override def writeAsync(persistentBatch: Seq[PersistentRepr]): Future[Unit] = {
    val futures = persistentBatch map { p =>
      import p._
      
      val request = new PutRequest(
        toBytes(Table),
        rowKey(processorId, sequenceNr),
        Family,
        Array(ProcessorId,          SequenceNr,          Marker,                  Message),
        Array(toBytes(processorId), toBytes(sequenceNr), toBytes(AcceptedMarker), persistentToBytes(p))
      )
      val deferred = asyncClient.put(request)
    
      val promise = Promise[Unit]()
      deferred.addCallback(new Callback[AnyRef, AnyRef] {
        def call(arg: AnyRef) = promise.complete(null)
      })
    
      promise.future
    }
    
    Future.sequence(futures).asInstanceOf[Future[Unit]]
  }

  // todo most probably racy internally... fix me
  override def deleteAsync(processorId: String, fromSequenceNr: Long, toSequenceNr: Long, permanent: Boolean): Future[Unit] = {
    val issueDelete =
      if (permanent) deleteRow _
      else markRowAsDeleted _

    Future {
      scan(processorId, fromSequenceNr, toSequenceNr) { res =>
        val key = res.getRow
        issueDelete(key)
      }
    } flatMap { deletes =>
      Future.sequence(deletes).asInstanceOf[Future[Unit]]
    }
  }

  /** WARNING: Plain HBase does not provide async APIs, thus this impl. only wraps the syncronous operations in a Future. */
  override def confirmAsync(processorId: String, sequenceNr: Long, channelId: String): Future[Unit] = {
    val request = new PutRequest(
      TableBytes,
      rowKey(processorId, sequenceNr),
      Family,
      Array(Marker),
      Array(confirmedMarkerBytes(channelId))
    )

    val p = Promise[Unit]()
    asyncClient.put(request).addCallback(new Callback[AnyRef, AnyRef] {
      def call(arg: AnyRef) = p.complete(null)
    })
    p.future
  }

  private def deleteRow(key: Array[Byte]): Future[Unit] = {
    val p = Promise[Unit]()
    asyncClient.delete(new DeleteRequest(TableBytes, key))
      .addCallback(new Callback[AnyRef, AnyRef] {
      def call(arg: AnyRef) = p.complete(null)
    })
    p.future
  }

  private def markRowAsDeleted(key: Array[Byte]): Future[Unit] = {
    val p = Promise[Unit]()
    asyncClient.put(
      new PutRequest(
        TableBytes,
        key,
        Family,
        Array(Marker),
        Array(DeletedMarkerBytes)
      )
    ).addCallback(new Callback[AnyRef, AnyRef] {
      def call(arg: AnyRef) = p.complete(null)
    })
    p.future
  }

  override def postStop(): Unit = {
    super.postStop()
    asyncClient.shutdown()
  }
}
