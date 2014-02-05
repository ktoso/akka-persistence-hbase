package akka.persistence.hbase.common

import akka.actor.Actor
import akka.persistence.{Persistent, PersistentRepr}
import akka.serialization.SerializationExtension

trait HBaseSerialization {
  self: Actor =>

  lazy val serialization = SerializationExtension(context.system)

  protected def persistentFromBytes(bytes: Array[Byte]): PersistentRepr =
    serialization.deserialize(bytes, classOf[PersistentRepr]).get

  protected def persistentToBytes(msg: Persistent): Array[Byte] =
    serialization.serialize(msg).get

}
