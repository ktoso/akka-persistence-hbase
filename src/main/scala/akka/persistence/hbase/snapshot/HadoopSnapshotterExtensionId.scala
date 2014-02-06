package akka.persistence.hbase.snapshot

import akka.actor._
import akka.persistence.hbase.journal.{PluginPersistenceSettings, HBaseClientFactory, HBaseJournalInit}
import akka.persistence.PersistenceSettings
import scala.Predef._
import org.apache.hadoop.conf.Configuration

object HadoopSnapshotterExtensionId extends ExtensionId[HadoopSnapshotter]
  with ExtensionIdProvider {

  val SnapshotStoreImplKey = "hadoop-snapshot-store.impl"

  override def lookup() = HadoopSnapshotterExtensionId

  override def createExtension(system: ExtendedActorSystem) = {
    val config = system.settings.config
    val snapshotterImpl = config.getString(SnapshotStoreImplKey)

    val pluginPersistenceSettings = PluginPersistenceSettings(config)
    val persistenceSettings = new PersistenceSettings(config.getConfig("akka.persistence"))

    val client = HBaseClientFactory.getClient(pluginPersistenceSettings, persistenceSettings)

    val HBaseSnapshotterName = classOf[HBaseSnapshotter].getCanonicalName
    val HdfsSnapshotterName = classOf[HdfsSnapshotter].getCanonicalName

    snapshotterImpl match {
      case HBaseSnapshotterName =>
        system.log.info("Using {} snapshotter implementation", HBaseSnapshotterName)
        new HBaseSnapshotter(system, pluginPersistenceSettings, client)

      case HdfsSnapshotterName =>
        system.log.info("Using {} snapshotter implementation", HdfsSnapshotterName)
        new HdfsSnapshotter(system, pluginPersistenceSettings)

      case other =>
        throw new IllegalStateException(s"$SnapshotStoreImplKey must be set to either $HBaseSnapshotterName or $HdfsSnapshotterName! Was: $other")
    }
  }
}
