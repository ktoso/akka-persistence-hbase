package akka.persistence.hbase.snapshot

import akka.actor._
import akka.persistence.hbase.journal.{HBasePersistenceSettings, HBaseClientFactory, HBaseJournalInit}
import akka.persistence.PersistenceSettings
import scala.Predef._

object HadoopSnapshotterExtensionId extends ExtensionId[HadoopSnapshotter]
  with ExtensionIdProvider {

  val SnapshotStoreImplKey = "hadoop-snapshot-store.impl"

  override def lookup() = HadoopSnapshotterExtensionId

  override def createExtension(system: ExtendedActorSystem) = {
    val config = system.settings.config
    val snapshotterImpl = config.getString(SnapshotStoreImplKey)

    val hBasePersistenceSettings = HBasePersistenceSettings(config)
    val persistenceSettings = new PersistenceSettings(config.getConfig("akka.persistence"))

    val client = HBaseClientFactory.getClient(hBasePersistenceSettings, persistenceSettings)

    val HBaseSnapshotterName = classOf[HBaseSnapshotter].getCanonicalName
    val HdfsSnapshotterName = classOf[HdfsSnapshotter].getCanonicalName

    snapshotterImpl match {
      case HBaseSnapshotterName =>
        new HBaseSnapshotter(system, hBasePersistenceSettings, client)

      case HdfsSnapshotterName =>
        new HdfsSnapshotter()

      case other =>
        throw new IllegalStateException(s"$SnapshotStoreImplKey must be set to either $HBaseSnapshotterName or $HdfsSnapshotterName! Was: $other")
    }
  }
}
