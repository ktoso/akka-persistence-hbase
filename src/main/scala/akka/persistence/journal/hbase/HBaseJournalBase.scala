package akka.persistence.journal.hbase

import akka.persistence._
import org.apache.hadoop.hbase.util.Bytes
import akka.serialization.SerializationExtension
import HBaseJournalInit._
import akka.actor.{Actor, ActorLogging}

trait HBaseJournalBase {
  this: Actor with ActorLogging with HBaseAsyncReplay with PersistenceMarkers =>

  import Bytes._

  val serialization = SerializationExtension(context.system)

  val config = context.system.settings.config.getConfig("hbase-journal")
  val hadoopConfig = getHBaseConfig(config)

  val scanBatchSize = config.getInt("scan-batch-size")

  val replayDispatcherId = config.getString("replay-dispatcher")

  /**
   * Number of regions the used Table is partitioned to.
   * '''MUST NOT change in the lifetime of this table.'''
   *
   * Should be a bigger number, for example 10 even if you currently have 2 regions, so you can split regions in the future.
   */
  val partitionCount: Int = config.getInt("partition.count")

  val Table = config.getString("table")
  val TableBytes = toBytes(Table)

  /** Used to avoid writing all data to the same region - see "hot region" problem */
  def partition(sequenceNr: Long): Long = sequenceNr % partitionCount

  @inline def padded(l: Long, howLong: Int) = String.valueOf(l).reverse.padTo(howLong, "0").reverse.mkString

  case class RowKey(processorId: String, sequenceNr: Long) {
    val part = partition(sequenceNr)
    val toBytes = Bytes.toBytes(s"${padded(part, 3)}-$processorId-${padded(sequenceNr, 20)}")
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
