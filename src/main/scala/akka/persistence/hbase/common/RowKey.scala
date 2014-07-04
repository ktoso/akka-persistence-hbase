package akka.persistence.hbase.common

import org.apache.hadoop.hbase.util.Bytes
import akka.persistence.hbase.journal.PluginPersistenceSettings

import scala.annotation.tailrec

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

  def firstInPartition(persistenceId: String, partition: Long)(implicit journalConfig: PluginPersistenceSettings) = {
    require(partition > 0, "partition must be > 0")
    require(partition <= journalConfig.partitionCount, "partition must be <= partitionCount")

    if (partition == journalConfig.partitionCount)
      RowKey.apply(persistenceId, partition)
    else
      RowKey.apply(persistenceId, partition % journalConfig.partitionCount)
  }

  def lastInPartition(persistenceId: String, partition: Long)(implicit journalConfig: PluginPersistenceSettings) = {
    require(partition > 0, s"partition must be > 0, ($partition)")
    require(partition <= journalConfig.partitionCount, s"partition must be <= partitionCount, ($partition <!= ${journalConfig.partitionCount})")

    val p = partition
    new RowKey(persistenceId, lastSeqNrInPartition(partition)) {
      override def part = p % journalConfig.partitionCount
    }
  }

  /** INTERNAL API */
  @tailrec private[hbase] def lastSeqNrInPartition(p: Long, i: Long = Long.MaxValue): Long = if (i % p == 0) i else lastSeqNrInPartition(p, i - 1)

  /** First key possible, similar to: `000-id-000000000000000000000` */
  def firstForProcessor(persistenceId: String)(implicit journalConfig: PluginPersistenceSettings) =
    RowKey(persistenceId, 0)

  /**
   * Last key prepared for Scan, similar to: `999-id-0000000121212`,
   * where the 999 is adjusted such, that a full scan reaches where it has to, and not further.
   */
  def lastForProcessorScan(persistenceId: String, upToSequenceNr: Long)(implicit journalConfig: PluginPersistenceSettings) =
      new RowKey(persistenceId, upToSequenceNr) {
        val partitionCount = hBasePersistenceSettings.partitionCount

        // todo this is wrong
        override def toKeyString =
          if (upToSequenceNr < partitionCount)
            super.toKeyString
          else
            s"${padded(partitionCount - 1, 3)}-$persistenceId-${padded(sequenceNr, 20)}" // partitionCount - 1 because we're using "N modulo partitionCount", so if equal => 0
  }

  /** Last key possible, similar to: `999-id-Long.MaxValue` */
  def lastForProcessor(persistenceId: String)(implicit journalConfig: PluginPersistenceSettings) =
    RowKey(persistenceId, 0)
}