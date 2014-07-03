package akka.persistence.hbase.common

import org.apache.hadoop.hbase.util.Bytes
import akka.persistence.hbase.journal.PluginPersistenceSettings

case class RowKey(persistenceId: String, sequenceNr: Long)(implicit val hBasePersistenceSettings: PluginPersistenceSettings) {

  def part = partition(sequenceNr)
  def toBytes = Bytes.toBytes(toKeyString)

  def toKeyString = s"${padded(part, 3)}-$persistenceId-${padded(sequenceNr, 20)}"

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
  def patternForProcessor(persistenceId: String)(implicit journalConfig: PluginPersistenceSettings) = s""".*-$persistenceId-.*"""

  /** First key possible, similar to: `0-id-000000000000000000000` */
  def firstForProcessor(persistenceId: String)(implicit journalConfig: PluginPersistenceSettings) =
    RowKey(persistenceId, 0)

  /** Last key prepared for Scan, similar to: `999-id-0000000121212` */
  def lastForProcessorScan(persistenceId: String, upToSequenceNr: Long)(implicit journalConfig: PluginPersistenceSettings) =
    new RowKey(persistenceId, upToSequenceNr) {
      override def toKeyString = s"${padded(hBasePersistenceSettings.partitionCount, 3)}-$persistenceId-${padded(sequenceNr, 20)}"
    }

  /** Last key possible, similar to: `999-id-Long.MaxValue` */
  def lastForProcessor(persistenceId: String)(implicit journalConfig: PluginPersistenceSettings) =
    RowKey(persistenceId, 0)
}