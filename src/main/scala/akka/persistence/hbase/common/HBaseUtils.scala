package akka.persistence.hbase.common

import akka.persistence.hbase.common.Columns._
import akka.persistence.hbase.journal.PersistencePluginSettings
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.util.Bytes

trait HBaseUtils {

  def hBasePersistenceSettings: PersistencePluginSettings

  def preparePartitionScan(table: Array[Byte], family: Array[Byte], startScanKey: RowKey, stopScanKey: RowKey, persistenceIdRowRegex: String, onlyRowKeys: Boolean): Scan = {
    val scan = new Scan
    scan.setStartRow(startScanKey.toBytes) // inclusive
    scan.setStopRow(stopScanKey.toBytes) // exclusive
    scan.setBatch(hBasePersistenceSettings.scanBatchSize)

    val filter = if (onlyRowKeys) {
      val fl = new FilterList()
      fl.addFilter(new FirstKeyOnlyFilter)
      fl.addFilter(new KeyOnlyFilter)
      fl.addFilter(new RowFilter(CompareOp.EQUAL, new RegexStringComparator(persistenceIdRowRegex)))
      fl
    } else {
      scan.addColumn(family, Marker)
      scan.addColumn(family, Message)

      new RowFilter(CompareOp.EQUAL, new RegexStringComparator(persistenceIdRowRegex))
    }

    scan.setFilter(filter)
    scan
  }

  /**
   * For snapshots
   */
  def preparePrefixScan(table: Array[Byte], family: Array[Byte], startScanKey: SnapshotRowKey, stopScanKey: SnapshotRowKey, persistenceIdPrefix: String, onlyRowKeys: Boolean): Scan = {
    preparePrefixScan(table, family, startScanKey.toBytes, stopScanKey.toBytes, persistenceIdPrefix, onlyRowKeys)
  }

  def preparePrefixScan(table: Array[Byte], family: Array[Byte], startScanKey: Array[Byte], stopScanKey: Array[Byte], persistenceIdPrefix: String, onlyRowKeys: Boolean): Scan = {
    val scan = new Scan
    scan.setStartRow(startScanKey) // inclusive
    scan.setStopRow(stopScanKey) // exclusive
    scan.setBatch(hBasePersistenceSettings.scanBatchSize)

    scan.setFilter(new PrefixFilter(Bytes.toBytes(persistenceIdPrefix)))

    scan
  }

}
