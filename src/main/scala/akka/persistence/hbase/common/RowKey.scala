package akka.persistence.hbase.common

import org.apache.hadoop.hbase.util.Bytes
import akka.persistence.hbase.journal.PluginPersistenceSettings

case class RowKey(processorId: String, sequenceNr: Long)(implicit hBasePersistenceSettings: PluginPersistenceSettings) {

  def part = partition(sequenceNr)
  val toBytes = Bytes.toBytes(toKeyString)

  def toKeyString = s"${padded(part, 3)}-$processorId-${padded(sequenceNr, 20)}"

  @inline def padded(l: Long, howLong: Int) =
    String.valueOf(l).reverse.padTo(howLong, "0").reverse.mkString

  /** Used to avoid writing all data to the same region - see "hot region" problem */
  private def partition(sequenceNr: Long): Long =
    sequenceNr % hBasePersistenceSettings.partitionCount
}

object RowKey {
  /**
   * Since we're salting (prefixing) the entries with partition numbers,
   * we must use this pattern for scanning for "all messages for processorX"
   */
  def patternForProcessor(processorId: String)(implicit journalConfig: PluginPersistenceSettings) = s""".*-$processorId-.*"""

  /** First key possible, similar to: `0-id-000000000000000000000` */
  def firstForProcessor(processorId: String)(implicit journalConfig: PluginPersistenceSettings) =
    RowKey(processorId, 0)

  /** Last key possible, similar to: `999-id-Long.MaxValue` */
  def lastForProcessor(processorId: String)(implicit journalConfig: PluginPersistenceSettings) =
    RowKey(processorId, 0)
}