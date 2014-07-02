package akka.persistence.hbase.common

import org.apache.hadoop.hbase.util.Bytes._

object Columns {
  val PersistenceId = toBytes("persistenceId")
  val SequenceNr  = toBytes("sequenceNr")
  val Marker      = toBytes("marker")
  val Message     = toBytes("payload")
}