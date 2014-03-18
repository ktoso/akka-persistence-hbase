package akka.persistence.hbase.journal

import com.typesafe.config.Config
import org.apache.hadoop.conf.Configuration

/**
 *
 * @param table table to be used to store akka messages
 * @param family column family name to store akka messages in
 * @param partitionCount Number of regions the used Table is partitioned to.
 *                       Currently must be FIXED, and not change during the lifetime of the app.
 *                       Should be a bigger number, for example 10 even if you currently have 2 regions, so you can split regions in the future.
 * @param scanBatchSize when performing scans, how many items to we want to obtain per one next(N) call
 * @param replayDispatcherId dispatcher for fetching and replaying messages
 */
case class PersistencePluginSettings(
  zookeeperQuorum: String,
  zookeeperParent: String,
  table: String,
  family: String,
  partitionCount: Int,
  scanBatchSize: Int,
  pluginDispatcherId: String,
  replayDispatcherId: String,
  publishTestingEvents: Boolean,
  snapshotTable: String,
  snapshotFamily: String,
  snapshotHdfsDir: String,
  hadoopConfiguration: Configuration
)

object PersistencePluginSettings {
  def apply(rootConfig: Config): PersistencePluginSettings = {
    val journalConfig = rootConfig.getConfig("hbase-journal")
    val snapshotConfig = rootConfig.getConfig("hadoop-snapshot-store")

    PersistencePluginSettings(
      zookeeperQuorum      = journalConfig.getString("hadoop-pass-through.hbase.zookeeper.quorum"),
      zookeeperParent      = journalConfig.getString("hadoop-pass-through.zookeeper.znode.parent"),
      table                = journalConfig.getString("table"),
      family               = journalConfig.getString("family"),
      partitionCount       = journalConfig.getInt("partition.count"),
      scanBatchSize        = journalConfig.getInt("scan-batch-size"),
      pluginDispatcherId   = journalConfig.getString("plugin-dispatcher"),
      replayDispatcherId   = journalConfig.getString("replay-dispatcher"),
      publishTestingEvents = journalConfig.getBoolean("publish-testing-events"),
      snapshotTable        = snapshotConfig.getString("hbase.table"),
      snapshotFamily       = snapshotConfig.getString("hbase.family"),
      snapshotHdfsDir      = snapshotConfig.getString("snapshot-dir"),
      hadoopConfiguration  = if (rootConfig ne null) HBaseJournalInit.getHBaseConfig(rootConfig) else null
    )
  }
}
