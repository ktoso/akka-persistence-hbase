package akka.persistence.journal.hbase

import akka.persistence.PersistentRepr
import org.apache.hadoop.hbase.client.Result
import akka.actor.{ActorLogging, Actor}
import scala.concurrent.Future

trait HBaseAsyncReplay {
  this: Actor with ActorLogging with HBaseJournalBase with PersistenceMarkers =>

  private lazy val replayDispatcherId = config.getString("replay-dispatcher")
  private implicit lazy val replayDispatcher = context.system.dispatchers.lookup(replayDispatcherId)

  def replayAsync(processorId: String, fromSequenceNr: Long, toSequenceNr: Long)(replayCallback: (PersistentRepr) => Unit): Future[Long] = {
    Future {
      log.debug(s"Async replay for processorId [$processorId], from sequenceNr: [$fromSequenceNr], to sequenceNr: [$toSequenceNr]")
      val all = scan(processorId, fromSequenceNr, toSequenceNr) {
        replay(replayCallback)(_)
      }

      (0L :: all).max
    }
  }

  def replay(replayCallback: (PersistentRepr) => Unit)(result: Result): Long = {
    import Columns._
    var msg = persistentFromBytes(result.getValue(Family, Message))

    val marker = String.valueOf(result.getValue(Family, Marker))
    marker match {
      case AcceptedMarker =>
        replayCallback(msg)

      case DeletedMarker =>
        msg = msg.update(deleted = true)

      case _ =>
        val channelId = extractSeqNrFromConfirmedMarker(marker)
        msg = msg.update(confirms = channelId +: msg.confirms)
    }

    replayCallback(msg)
    msg.sequenceNr
  }

}
