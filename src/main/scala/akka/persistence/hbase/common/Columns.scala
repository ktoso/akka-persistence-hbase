package akka.persistence.hbase.common

import org.apache.hadoop.hbase.util.Bytes._

object Columns {
  val ProcessorId = toBytes("processorId")
  val SequenceNr  = toBytes("sequenceNr")
  val Marker      = toBytes("marker")
  val Message     = toBytes("payload")
}