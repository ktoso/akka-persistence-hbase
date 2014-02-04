package akka.persistence.hbase.snapshot

import akka.actor._

object HadoopSnapshotterExtension extends ExtensionId[HadoopSnapshotter]
  with ExtensionIdProvider {
  
  override def lookup() = this

  override def createExtension(system: ExtendedActorSystem) = new HdfsSnapshotter
}
