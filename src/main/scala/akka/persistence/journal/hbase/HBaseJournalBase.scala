package akka.persistence.journal.hbase

import akka.persistence._
import org.apache.hadoop.hbase.util.Bytes
import akka.serialization.SerializationExtension
import HBaseJournalInit._
import akka.actor.{Actor, ActorLogging}

trait HBaseJournalBase extends HBaseAsyncRecovery {
  this: Actor with ActorLogging with HBaseAsyncWriteJournal =>

  import Bytes._

  val serialization = SerializationExtension(context.system)

  val config = context.system.settings.config.getConfig("hbase-journal")
  val journalConfig = HBaseJournalConfig(config)
  val hadoopConfig = getHBaseConfig(config)

  val Table = config.getString("table")
  val TableBytes = toBytes(Table)

  /** Used to avoid writing all data to the same region - see "hot region" problem */
  def partition(sequenceNr: Long): Long = sequenceNr % journalConfig.partitionCount

  @inline def padded(l: Long, howLong: Int) =
    String.valueOf(l).reverse.padTo(howLong, "0").reverse.mkString

  case class RowKey(processorId: String, sequenceNr: Long) {
    val part = partition(sequenceNr)
    val toBytes = Bytes.toBytes(s"${padded(part, 3)}-$processorId-${padded(sequenceNr, 20)}")
  }
  object RowKey {
    /**
     * Since we're salting (prefixing) the entries with partition numbers,
     * we must use this pattern for scanning for "all messages for processorX"
     */
    def patternForProcessor(processorId: String) = s""".*-$processorId-.*"""
  }

  object Columns {
    val Family = toBytes(config.getString("family"))

    val ProcessorId = toBytes("processorId")
    val SequenceNr  = toBytes("sequenceNr")
    val Marker      = toBytes("marker")
    val Message     = toBytes("payload")
  }

  protected def persistentFromBytes(bytes: Array[Byte]): PersistentRepr =
    serialization.deserialize(bytes, classOf[PersistentRepr]).get

  protected def persistentToBytes(msg: Persistent): Array[Byte] =
    serialization.serialize(msg).get

}
