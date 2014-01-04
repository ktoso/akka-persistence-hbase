package akka.persistence.journal.hbase

import org.apache.hadoop.hbase.util.Bytes

trait PersistenceMarkers {

  import Bytes._

  val AcceptedMarker = "A"
  val AcceptedMarkerBytes = toBytes(AcceptedMarker)

  val DeletedMarker = "D"
  val DeletedMarkerBytes = toBytes(DeletedMarker)

  def confirmedMarker(channelId: String) = s"C-$channelId"
  def confirmedMarkerBytes(channelId: String) = toBytes(confirmedMarker(channelId))

  def extractSeqNrFromConfirmedMarker(marker: String): String = marker.substring(2)

}
