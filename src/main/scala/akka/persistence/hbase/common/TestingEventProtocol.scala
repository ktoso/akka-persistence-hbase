package akka.persistence.hbase.common

import akka.persistence.SnapshotSelectionCriteria

/**
 * Grouped events which will be sent to the `eventStream` if `publish-testing-events` is enabeled.
 */
object TestingEventProtocol {

  private[hbase] case class FinishedWrites(written: Int)

  private[hbase] case class DeletedSnapshotsFor(processorId: String, criteria: SnapshotSelectionCriteria)

}
