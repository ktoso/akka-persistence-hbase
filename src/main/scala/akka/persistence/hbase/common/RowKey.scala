package akka.persistence.hbase.common

import org.apache.hadoop.hbase.util.Bytes
import akka.persistence.hbase.journal.PersistencePluginSettings

import scala.annotation.tailrec

case class RowKey(part: Long, persistenceId: String, sequenceNr: Long)(implicit val hBasePersistenceSettings: PersistencePluginSettings) {

  def toBytes = Bytes.toBytes(toKeyString)

  def toKeyString = s"${padded(part, 3)}-$persistenceId-${padded(sequenceNr, 20)}"

  @inline def padded(l: Long, howLong: Int) =
    String.valueOf(l).reverse.padTo(howLong, "0").reverse.mkString
}

case class SnapshotRowKey(persistenceId: String, sequenceNr: Long) {

  def toBytes = Bytes.toBytes(toKeyString)

  def toKeyString = s"$persistenceId-${padded(sequenceNr, 20)}"

  @inline def padded(l: Long, howLong: Int) =
    String.valueOf(l).reverse.padTo(howLong, "0").reverse.mkString
}

object RowKey {

  /**
   * Since we're salting (prefixing) the entries with selectPartition numbers,
   * we must use this pattern for scanning for "all messages for processorX"
   */
  def patternForProcessor(persistenceId: String)(implicit journalConfig: PersistencePluginSettings) = s""".*-$persistenceId-.*"""

  def firstInPartition(persistenceId: String, partition: Long, fromSequenceNr: Long = 0)(implicit journalConfig: PersistencePluginSettings) = {
    require(partition > 0, "partition must be > 0")
    require(partition <= journalConfig.partitionCount, "partition must be <= partitionCount")

    val lowerBoundAdjustedSeqNr =
      if (partition < fromSequenceNr)
        fromSequenceNr
      else
        selectPartition(partition)

      RowKey.apply(selectPartition(partition), persistenceId, lowerBoundAdjustedSeqNr)
  }

  def lastInPartition(persistenceId: String, partition: Long, toSequenceNr: Long = Long.MaxValue)(implicit journalConfig: PersistencePluginSettings) = {
    require(partition > 0, s"partition must be > 0, ($partition)")
    require(partition <= journalConfig.partitionCount, s"partition must be <= partitionCount, ($partition <!= ${journalConfig.partitionCount})")
    require(toSequenceNr >= 0, s"toSequenceNr must be >= 0, ($toSequenceNr)")

    new RowKey(selectPartition(partition)(journalConfig), persistenceId, toSequenceNr)
  }

  def lastInPartition(persistenceId: String, partition: Long)(implicit journalConfig: PersistencePluginSettings) = {
    require(partition > 0, s"partition must be > 0, ($partition)")
    require(partition <= journalConfig.partitionCount, s"partition must be <= partitionCount, ($partition <!= ${journalConfig.partitionCount})")

    new RowKey(selectPartition(partition)(journalConfig), persistenceId, lastSeqNrInPartition(partition))
  }

  /** First key possible, similar to: `000-id-000000000000000000000` */
  def firstForPersistenceId(persistenceId: String)(implicit journalConfig: PersistencePluginSettings) =
    RowKey(0, persistenceId, 0)

  /** Last key possible, similar to: `999-id-Long.MaxValue` */
  def lastForPersistenceId(persistenceId: String, toSequenceNr: Long = Long.MaxValue)(implicit journalConfig: PersistencePluginSettings) =
    lastInPartition(persistenceId, selectPartition(journalConfig.partitionCount), toSequenceNr)

  /** Used to avoid writing all data to the same region - see "hot region" problem */
  def selectPartition(sequenceNr: Long)(implicit journalConfig: PersistencePluginSettings): Long =
    if (sequenceNr % journalConfig.partitionCount == 0)
      journalConfig.partitionCount
    else
      sequenceNr % journalConfig.partitionCount

  val RowKeyPattern = """\d+-.*-\d""".r

  def extractSeqNr(rowKey: Array[Byte]): Long = {
    val num = rowKey.reverse.takeWhile(_.toChar.isDigit).reverse // todo: make faster
    Bytes.toString(num).toLong
  }


  /** INTERNAL API */
  @tailrec private[hbase] def lastSeqNrInPartition(p: Long, i: Long = Long.MaxValue): Long = if (i % p == 0) i else lastSeqNrInPartition(p, i - 1)

}

object SnapshotRowKey {

  def firstForPersistenceId(persistenceId: String): SnapshotRowKey =
    SnapshotRowKey(persistenceId, 0)

  def lastForPersistenceId(persistenceId: String, toSequenceNr: Long = Long.MaxValue): SnapshotRowKey =
    SnapshotRowKey(persistenceId, toSequenceNr)
}