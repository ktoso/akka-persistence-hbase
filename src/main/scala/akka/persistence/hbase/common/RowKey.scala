package akka.contrib.persistence.hbase.common

import org.apache.hadoop.hbase.util.Bytes
import akka.contrib.persistence.hbase.journal.HBaseJournalConfig

case class RowKey(processorId: String, sequenceNr: Long) {

  def part = partition(sequenceNr)
  val toBytes = Bytes.toBytes(toKeyString)

  def toKeyString = s"${padded(part, 3)}-$processorId-${padded(sequenceNr, 20)}"

  @inline def padded(l: Long, howLong: Int) =
    String.valueOf(l).reverse.padTo(howLong, "0").reverse.mkString

  /** Used to avoid writing all data to the same region - see "hot region" problem */
  private def partition(sequenceNr: Long)(implicit journalConfig: HBaseJournalConfig): Long =
    sequenceNr % journalConfig.partitionCount
}

object RowKey {
  /**
   * Since we're salting (prefixing) the entries with partition numbers,
   * we must use this pattern for scanning for "all messages for processorX"
   */
  def patternForProcessor(processorId: String) = s""".*-$processorId-.*"""

  /** First key possible, similar to: `0-id-000000000000000000000` */
  def firstForProcessor(processorId: String) =
    RowKey(processorId, 0)

  /** Last key possible, similar to: `999-id-Long.MaxValue` */
  def lastForProcessor(processorId: String) =
    RowKey(processorId, 0)
}