package akka.persistence.journal.hbase

import akka.persistence._
import scala.collection.immutable.Seq
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import scala.concurrent._
import akka.serialization.SerializationExtension
import HBaseJournalInit._
import scala.collection.mutable.ListBuffer
import akka.actor.{Actor, ActorLogging}
import org.apache.hadoop.hbase.ipc.HBaseClient
import org.apache.hadoop.hbase.io.ImmutableBytesWritable

trait HBaseJournalBase {
  this: Actor with ActorLogging with HBaseAsyncReplay with PersistenceMarkers =>

  import Bytes._
  import collection.JavaConverters._

  import context.dispatcher

  val serialization = SerializationExtension(context.system)

  val config = context.system.settings.config.getConfig("hbase-journal")
  val hadoopConfig = getHBaseConfig(config)

  val scanBatchSize = config.getInt("scan-batch-size")


  val Table = config.getString("table")
  val TableBytes = toBytes(Table)

  val table = new HTable(hadoopConfig, Table)

  protected def rowKey(processorId: String, sequenceNr: Long): Array[Byte] = {
    @inline def padded(l: Long) = String.valueOf(l).reverse.padTo(20, "0").reverse.mkString
    toBytes(s"$processorId-${padded(sequenceNr)}")
  }

  object Columns {
    val Family = toBytes(config.getString("family"))

    val ProcessorId = toBytes("processorId")
    val SequenceNr = toBytes("sequenceNr")
    val Marker = toBytes("marker")
    val Message = toBytes("payload")
  }
  import Columns._

  def write(persistentBatch: Seq[PersistentRepr]): Unit = {
    val puts = preparePuts(persistentBatch)

    table.batch(puts.asJava)
  }


  protected def preparePuts(persistentBatch: Seq[PersistentRepr]): Seq[Put] = {
    persistentBatch map { p =>
      import p._

      val put = new Put(rowKey(p.processorId, sequenceNr))
      put.add(Family, ProcessorId, toBytes(p.processorId))
      put.add(Family, SequenceNr, toBytes(sequenceNr))
      put.add(Family, Marker, AcceptedMarkerBytes)
      put.add(Family, Message, persistentToBytes(p))
    }
  }

  def delete(processorId: String, fromSequenceNr: Long, toSequenceNr: Long, permanent: Boolean): Unit = {
    scan(processorId, fromSequenceNr, toSequenceNr) { res =>
      val row = res.getRow

      log.debug(s"Prepare Delete for (permanent: $permanent) row: ${Bytes.toString(row)}")

      if (permanent) {
        table.delete(new Delete(row))
      } else {
        val put = new Put(row)
        put.add(Family, Marker, DeletedMarkerBytes)
        table.put(put)
      }
    }
  }

  def confirm(processorId: String, sequenceNr: Long, channelId: String): Unit = {
    log.debug(s"Confirm message for processorId:[$processorId], sequenceNr:$sequenceNr, marker: ${confirmedMarker(channelId)}")

    val p = new Put(rowKey(processorId, sequenceNr))
    p.add(Family, Marker, confirmedMarkerBytes(channelId))
    table.put(p)
  }

  protected def scan[T](processorId: String, fromSequenceNr: Long, toSequenceNr: Long)(callback: Result => T): List[T] = {
    val toRow = rowKey(processorId, toSequenceNr)
    val fromRow = rowKey(processorId, fromSequenceNr)

    log.debug(s"Scan for messages between row keys: ${Bytes.toString(fromRow)} - ${Bytes.toString(toRow)}")

    val s = new Scan(fromRow, toRow)
    s.addFamily(Columns.Family)

    val scanner = table.getScanner(s)
    val returns = ListBuffer[T]()
    try {
      var res: Array[Result] = Array()
      do {
        res = scanner.next(scanBatchSize)

        res map { r =>
          returns append callback(r)
        }
      } while (!res.isEmpty)
    } finally {
      scanner.close()
    }

    returns.toList
  }

  protected def persistentFromBytes(bytes: Array[Byte]): PersistentRepr =
    serialization.deserialize(bytes, classOf[PersistentRepr]).get

  protected def persistentToBytes(msg: Persistent): Array[Byte] =
    serialization.serialize(msg).get


  override def postStop(): Unit = {
    log.info(s"Stopping hbase HBase Table (${config.getString("table")}) connection...")
    table.close()
  }


}
