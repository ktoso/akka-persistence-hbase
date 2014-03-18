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
case class PluginPersistenceSettings(
  zookeeperQuorum: String,
  zookeeperParent: String,
  table: String,
  family: String,
  partitionCount: Int,
  scanBatchSize: Int,
  pluginDispatcherId: String,
  replayDispatcherId: String,
  publishTestingEvents: Boolean,
  snapshotHdfsDir: String,
  hadoopConfiguration: Configuration
)

object PluginPersistenceSettings {
  def apply(rootConfig: Config): PluginPersistenceSettings = {
    val journalConfig = rootConfig.getConfig("hbase-journal")
    val snapshotConfig = rootConfig.getConfig("hadoop-snapshot-store")

    PluginPersistenceSettings(
      zookeeperQuorum      = journalConfig.getString("hbase.hbase.zookeeper.quorum"),
      zookeeperParent      = journalConfig.getString("hbase.zookeeper.znode.parent"),
      table                = journalConfig.getString("table"),
      family               = journalConfig.getString("family"),
      partitionCount       = journalConfig.getInt("partition.count"),
      scanBatchSize        = journalConfig.getInt("scan-batch-size"),
      pluginDispatcherId   = journalConfig.getString("plugin-dispatcher"),
      replayDispatcherId   = journalConfig.getString("replay-dispatcher"),
      publishTestingEvents = journalConfig.getBoolean("publish-testing-events"),
      snapshotHdfsDir      = snapshotConfig.getString("snapshot-dir"),
      hadoopConfiguration  = HBaseJournalInit.getHBaseConfig(rootConfig)
    )
  }
}
