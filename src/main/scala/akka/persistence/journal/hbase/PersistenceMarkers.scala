package akka.persistence.journal.hbase

trait PersistenceMarkers {

  val AcceptedMarker = "A"

  val DeletedMarker = "D"

  def confirmedMarker(channelId: String): String = s"C-$channelId"

  def extractSeqNrFromConfirmedMarker(marker: String): String = marker.substring(2)

}
