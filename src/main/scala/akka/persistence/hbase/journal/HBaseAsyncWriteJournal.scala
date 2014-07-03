package akka.persistence.hbase.journal

import akka.actor.{Actor, ActorLogging}
import akka.persistence.hbase.common._
import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.{PersistentConfirmation, PersistenceSettings, PersistentId, PersistentRepr}

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

  implicit override val executionContext = context.system.dispatchers.lookup(hBasePersistenceSettings.pluginDispatcherId)


  import akka.persistence.hbase.common.Columns._
  import akka.persistence.hbase.common.DeferredConversions._
  import org.apache.hadoop.hbase.util.Bytes._

import scala.collection.JavaConverters._

  // journal plugin api impl -------------------------------------------------------------------------------------------

  override def asyncWriteMessages(persistentBatch: immutable.Seq[PersistentRepr]): Future[Unit] = {
    log.debug(s"Write async for {} presistent messages", persistentBatch.size)

    val futures = persistentBatch map { p =>
      import p._
      
      executePut(
        RowKey(persistenceId, sequenceNr).toBytes,
        Array(PersistenceId,          SequenceNr,          Marker,                  Message),
        Array(toBytes(persistenceId), toBytes(sequenceNr), toBytes(AcceptedMarker), persistentToBytes(p))
      )
    }

    flushWrites()
    Future.sequence(futures) map {
      case _ if publishTestingEvents => context.system.eventStream.publish(FinishedWrites(persistentBatch.size))
    }
  }

  override def asyncWriteConfirmations(confirmations: immutable.Seq[PersistentConfirmation]): Future[Unit] = {
    log.debug(s"AsyncWriteConfirmations for ${confirmations.size} messages")

    val fs = confirmations map { confirm =>
      confirmAsync(confirm.persistenceId, confirm.sequenceNr, confirm.channelId)
    }

    flushWrites()
    Future.sequence(fs)
  }

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

  override def asyncDeleteMessagesTo(persistenceId: String, toSequenceNr: Long, permanent: Boolean): Future[Unit] = {
    log.debug(s"AsyncDeleteMessagesTo for persistenceId: {} to sequenceNr: {}, premanent: {}", persistenceId, toSequenceNr, permanent)
    val doDelete = deleteFunctionFor(permanent)

    val scanner = newScanner()
    scanner.setStartKey(RowKey.firstForProcessor(persistenceId).toBytes)
    scanner.setStopKey(RowKey(persistenceId, toSequenceNr).toBytes)
    scanner.setKeyRegexp(RowKey.patternForProcessor(persistenceId))

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

    val fAll = go()
    fAll onComplete { case _ =>
      log.debug("Finished deleting messages for persistenceId: {}, to sequenceNr: {}, permanent: {}", persistenceId, toSequenceNr, permanent)
      context.system.eventStream.publish(FinishedDeletes(toSequenceNr))
    }
    fAll
  }

  // end of journal plugin api impl ------------------------------------------------------------------------------------

  def confirmAsync(persistenceId: String, sequenceNr: Long, channelId: String): Future[Unit] = {
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
