package akka.persistence.hbase.journal

import com.typesafe.config.Config

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
case class HBasePersistenceSettings(
  zookeeperQuorum: String,
  table: String,
  family: String,
  partitionCount: Int,
  scanBatchSize: Int,
  pluginDispatcherId: String,
  replayDispatcherId: String,
  publishTestingEvents: Boolean
)

object HBasePersistenceSettings {
  def apply(rootConfig: Config): HBasePersistenceSettings = {
    val journalConfig = rootConfig.getConfig("hbase-journal")

    HBasePersistenceSettings(
      zookeeperQuorum      = journalConfig.getString("hbase.zookeeper.quorum"),
      table                = journalConfig.getString("table"),
      family               = journalConfig.getString("family"),
      partitionCount       = journalConfig.getInt("partition.count"),
      scanBatchSize        = journalConfig.getInt("scan-batch-size"),
      pluginDispatcherId   = journalConfig.getString("plugin-dispatcher"),
      replayDispatcherId   = journalConfig.getString("replay-dispatcher"),
      publishTestingEvents = journalConfig.getBoolean("publish-testing-events")
    )
  }
}
