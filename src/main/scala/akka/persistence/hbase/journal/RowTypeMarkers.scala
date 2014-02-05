package akka.persistence.hbase.journal

import org.apache.hadoop.hbase.util.Bytes

object RowTypeMarkers {

  import Bytes._

  val AcceptedMarker = "A"
  val AcceptedMarkerBytes = toBytes(AcceptedMarker)

  val DeletedMarker = "D"
  val DeletedMarkerBytes = toBytes(DeletedMarker)

  val SnapshotMarker = "S"
  val SnapshotMarkerBytes = toBytes(SnapshotMarker)

  def confirmedMarker(channelId: String) = s"C-$channelId"
  def confirmedMarkerBytes(channelId: String) = toBytes(confirmedMarker(channelId))

  def extractSeqNrFromConfirmedMarker(marker: String): String = marker.substring(2)

}
