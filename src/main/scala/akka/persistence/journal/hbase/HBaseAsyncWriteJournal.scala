package akka.persistence.journal.hbase

import akka.persistence.journal.AsyncWriteJournal
import scala.collection.immutable.Seq
import akka.persistence.{Persistent, PersistentConfirmation, PersistentId, PersistentRepr}
import scala.concurrent._
import scala.concurrent.duration._
import akka.actor.ActorLogging
import org.hbase.async.{HBaseClient => AsyncBaseClient, KeyValue, DeleteRequest, PutRequest}
import org.apache.hadoop.hbase.util.Bytes
import com.stumbleupon.async.Callback
import java.{util => ju}
import java.util.concurrent.atomic.AtomicInteger
import scala.util.Success
import scala.collection.{mutable, immutable}
import java.util. { ArrayList => JArrayList }
import akka.persistence.journal.hbase.HBaseJournalInit._
import akka.serialization.SerializationExtension

/**
 * Asyncronous HBase Journal.
 *
 * Uses AsyncBase to implement asynchronous IPC with HBase.
 */
class HBaseAsyncWriteJournal extends HBaseJournalBase with AsyncWriteJournal
  with HBaseAsyncRecovery with PersistenceMarkers
  with DeferredConversions
  with ActorLogging {

  import HBaseAsyncWriteJournal._

  val serialization = SerializationExtension(context.system)

  val config = context.system.settings.config.getConfig("hbase-journal")

  val publish = journalConfig.publishTestingEvents
  import context.dispatcher

  import Bytes._
  import Columns._
  import collection.JavaConverters._

  val client = HBaseAsyncWriteJournal.getClient(journalConfig)

  // journal plugin api impl

  override def asyncWriteMessages(persistentBatch: immutable.Seq[PersistentRepr]): Future[Unit] = {
    log.debug(s"Write async for ${persistentBatch.size} presistent messages")

    val futures = persistentBatch map { p =>
      import p._
      
      executePut(
        RowKey(processorId, sequenceNr).toBytes,
        Array(ProcessorId,          SequenceNr,          Marker,                  Message),
        Array(toBytes(processorId), toBytes(sequenceNr), toBytes(AcceptedMarker), persistentToBytes(p))
      )
    }

    val f = Future.sequence(futures)
    if (publish) f map { _ => context.system.eventStream.publish(Finished(persistentBatch.size)) }
    f
  }

  override def asyncWriteConfirmations(confirmations: immutable.Seq[PersistentConfirmation]): Future[Unit] = {
    log.debug(s"AsyncWriteConfirmations for ${confirmations.size} messages")

    val fs = confirmations map { confirm =>
      confirmAsync(confirm.processorId, confirm.sequenceNr, confirm.channelId)
    }

    Future.sequence(fs)
  }

  override def asyncDeleteMessages(messageIds: immutable.Seq[PersistentId], permanent: Boolean): Future[Unit] = {
    log.debug(s"Async delete [${messageIds.size}] messages, premanent: $permanent")

    val doDelete = deleteFunctionFor(permanent)

    val deleteFutures = for {
      messageId <- messageIds
      rowId = RowKey(messageId.processorId, messageId.sequenceNr)
    } yield doDelete(rowId.toBytes)
    
    Future.sequence(deleteFutures)
  }

  override def asyncDeleteMessagesTo(processorId: String, toSequenceNr: Long, permanent: Boolean): Future[Unit] = {
    log.debug(s"AsyncDeleteMessagesTo for processorId: $processorId to sequenceNr: $toSequenceNr, premanent: $permanent")
    val doDelete = deleteFunctionFor(permanent)

    val scanner = newScanner()
    scanner.setStartKey(RowKey.firstForProcessor(processorId).toBytes)
    scanner.setStopKey(RowKey(processorId, toSequenceNr).toBytes)
    scanner.setKeyRegexp(RowKey.patternForProcessor(processorId))

    def handleRows(in: AnyRef): Future[Unit] = in match {
      case null =>
        log.debug("AsyncDeleteMessagesTo finished scanning for keys")
        scanner.close()
        Future(Array[Byte]())

      case rows: AsyncBaseRows  =>
        val deletes = for {
          row <- rows.asScala
          col <- row.asScala.headOption // just one entry is enough, because is contains the key
        } yield doDelete(col.key)

        go() flatMap { _ => Future.sequence(deletes) }
    }

    def go() = scanner.nextRows() flatMap handleRows

    go()
  }

  // end of journal plugin api impl

  def confirmAsync(processorId: String, sequenceNr: Long, channelId: String): Future[Unit] = {
      log.debug(s"Confirming async for processorId: $processorId, sequenceNr: $sequenceNr and channelId: $channelId")

      executePut(
        RowKey(processorId, sequenceNr).toBytes,
        Array(Marker),
        Array(confirmedMarkerBytes(channelId))
      )
    }

  private def deleteFunctionFor(permanent: Boolean): (Array[Byte]) => Future[Unit] = {
    if (permanent) deleteRow
    else markRowAsDeleted
  }

  // execute ops

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

  private def newScanner() = {
    val scanner = client.newScanner(Table)
    scanner.setFamily(Family)
    scanner
  }

  override def postStop(): Unit = {
    client.shutdown()
    super.postStop()
  }
}

object HBaseAsyncWriteJournal {

  case class Finished(written: Int)

  private var _zookeeperQuorum: String = _

  /** based on the docs, there should always be only one instance, reused even if we had more tables */
  private lazy val client = new AsyncBaseClient(_zookeeperQuorum)

  def getClient(config: HBaseJournalConfig) = {
    _zookeeperQuorum = config.zookeeperQuorum
    client.setFlushInterval(config.flushInterval)
    client
  }
}