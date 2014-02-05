package akka.persistence.hbase.journal

import org.hbase.async.HBaseClient
import akka.persistence.PersistenceSettings

object HBaseClientFactory {

  private var _zookeeperQuorum: String = _

  /** based on the docs, there should always be only one instance, reused even if we had more tables */
  private lazy val client = new HBaseClient(_zookeeperQuorum)

  /** Always returns the same client */
  def getClient(config: HBasePersistenceSettings, persistenceSettings: PersistenceSettings) = {
    _zookeeperQuorum = config.zookeeperQuorum

    // since we will be forcing a flush anyway after each batch, let's not make asyncbase flush more than it needs to.
    // for example, we tell akka "200", but asyncbase was set to "20", so it would flush way more often than we'd expect it to.
    // by setting the internal flushing to max(...), we're manually in hold of doing the flushing at the rigth moment.
    val maxBatchSize = List(
      persistenceSettings.journal.maxMessageBatchSize,
      persistenceSettings.journal.maxConfirmationBatchSize,
      persistenceSettings.journal.maxDeletionBatchSize
    ).max.toShort

    client.setFlushInterval(maxBatchSize)
    client
  }

}
