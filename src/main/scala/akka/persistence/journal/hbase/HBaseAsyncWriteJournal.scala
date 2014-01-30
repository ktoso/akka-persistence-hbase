package akka.persistence.journal.hbase

import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.{PersistenceSettings, PersistentConfirmation, PersistentId, PersistentRepr}
import scala.concurrent._
import akka.actor.ActorLogging
import org.hbase.async.{HBaseClient => AsyncBaseClient, DeleteRequest, PutRequest}
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.immutable
import akka.serialization.SerializationExtension
import akka.persistence.journal.hbase.util.DeferredConversions

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

  override val serialization = SerializationExtension(context.system)

  override val config = context.system.settings.config.getConfig("hbase-journal")
  
  private val persistenceSettings = new PersistenceSettings(context.system.settings.config.getConfig("akka.persistence"))

  private val publish = journalConfig.publishTestingEvents

  import context.dispatcher

  import Bytes._
  import Columns._
  import collection.JavaConverters._

  override val client = HBaseAsyncWriteJournal.getClient(journalConfig, persistenceSettings)
  
  // journal plugin api impl -------------------------------------------------------------------------------------------

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

    flushWrites()
    val f = Future.sequence(futures)
    if (publish) f map { _ => context.system.eventStream.publish(Finished(persistentBatch.size)) }
    f
  }

  override def asyncWriteConfirmations(confirmations: immutable.Seq[PersistentConfirmation]): Future[Unit] = {
    log.debug(s"AsyncWriteConfirmations for ${confirmations.size} messages")

    val fs = confirmations map { confirm =>
      confirmAsync(confirm.processorId, confirm.sequenceNr, confirm.channelId)
    }

    flushWrites()
    Future.sequence(fs)
  }

  override def asyncDeleteMessages(messageIds: immutable.Seq[PersistentId], permanent: Boolean): Future[Unit] = {
    log.debug(s"Async delete [${messageIds.size}] messages, premanent: $permanent")

    val doDelete = deleteFunctionFor(permanent)

    val deleteFutures = for {
      messageId <- messageIds
      rowId = RowKey(messageId.processorId, messageId.sequenceNr)
    } yield doDelete(rowId.toBytes)

    flushWrites()
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
        flushWrites()
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

  // end of journal plugin api impl ------------------------------------------------------------------------------------

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

  /**
   * Sends the buffered commands to HBase. Does not guarantee that they "complete" right away.
   */
  def flushWrites() {
    client.flush()
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

  def getClient(config: HBaseJournalConfig, persistenceSettings: PersistenceSettings) = {
    _zookeeperQuorum = config.zookeeperQuorum

    // since we will be forcing a flush anyway after each batch, let's not make asyncbase flush more than it needs to.
    // for example, we tell akka "200", but asyncbase was set to "20", so it would flush way more often than we'd expect it to.
    // by setting the internal flushing to max(...), we're manually in hold of doing the flushing at the rigth moment.
    val maxBatchSize = List(
      persistenceSettings.journal.maxMessageBatchSize,
      persistenceSettings.journal.maxConfirmationBatchSize,
      persistenceSettings.journal.maxDeletionBatchSize
    ).max.toShort

    client.setFlushInterval(maxBatchSize)
    client
  }
}