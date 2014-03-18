package akka.persistence.hbase.journal

import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.{PersistenceSettings, PersistentConfirmation, PersistentId, PersistentRepr}
import scala.concurrent._
import akka.actor.{Actor, ActorLogging}
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.immutable
import akka.serialization.SerializationExtension
import akka.persistence.hbase.common.{RowKey, DeferredConversions}
import akka.persistence.hbase.common._

/**
 * Asyncronous HBase Journal.
 *
 * Uses AsyncBase to implement asynchronous IPC with HBase.
 */
class HBaseAsyncWriteJournal extends Actor with ActorLogging
  with HBaseJournalBase with AsyncWriteJournal
  with HBaseAsyncRecovery {

  import RowTypeMarkers._
  import TestingEventProtocol._

  private lazy val config = context.system.settings.config

  implicit lazy val hBasePersistenceSettings = PluginPersistenceSettings(config)

  lazy val hadoopConfig = HBaseJournalInit.getHBaseConfig(config)

  lazy val client = HBaseClientFactory.getClient(hBasePersistenceSettings, new PersistenceSettings(config.getConfig("akka.persistence")))

  lazy val publishTestingEvents = hBasePersistenceSettings.publishTestingEvents

  implicit override val executionContext = context.system.dispatchers.lookup(hBasePersistenceSettings.pluginDispatcherId)


  import Bytes._
  import Columns._
  import DeferredConversions._
  import collection.JavaConverters._

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
    Future.sequence(futures) map {
      case _ if publishTestingEvents => context.system.eventStream.publish(FinishedWrites(persistentBatch.size))
      case _ =>
    }
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

  override def postStop(): Unit = {
    client.shutdown()
    super.postStop()
  }
}
