package akka.persistence.journal.hbase

import akka.persistence.journal.SyncWriteJournal
import akka.actor.ActorLogging

class HBaseSyncWriteJournal extends SyncWriteJournal with HBaseJournalBase
  with HBaseAsyncReplay with PersistenceMarkers
  with ActorLogging {

   override def postStop(): Unit = {
     super[HBaseJournalBase].postStop()
   }

}
