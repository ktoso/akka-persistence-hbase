package akka.persistence.hbase.common

import akka.persistence.serialization.Snapshot
import akka.persistence.{Persistent, PersistentRepr}
import akka.serialization.Serialization

trait HBaseSerialization {

  def serialization: Serialization


  protected def snapshotFromBytes(bytes: Array[Byte]): Snapshot =
    serialization.deserialize(bytes, classOf[Snapshot]).get

  protected def snapshotToBytes(msg: Snapshot): Array[Byte] =
    serialization.serialize(msg).get

  protected def persistentFromBytes(bytes: Array[Byte]): PersistentRepr =
    serialization.deserialize(bytes, classOf[PersistentRepr]).get

  protected def persistentToBytes(msg: Persistent): Array[Byte] =
    serialization.serialize(msg).get

}
