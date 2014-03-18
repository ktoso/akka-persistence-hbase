package akka.persistence.hbase.journal

import akka.persistence.journal.JournalSpec
import com.typesafe.config.ConfigFactory

/**
 * Plugin TCK (Martin's) Spec
 */
class AsyncJournalTCKSpec extends JournalSpec {

  // because of costy init of hbase-client, afterwards it's fast
  lazy val config = ConfigFactory.parseString("akka.test.timefactor=5").withFallback(ConfigFactory.load())

  lazy val settings = PersistencePluginSettings(config)

  override protected def beforeAll() {
    HBaseJournalInit.createTable(config, settings.table, settings.family)
    HBaseJournalInit.createTable(config, settings.snapshotTable, settings.snapshotFamily)

    super.beforeAll()
  }

  override protected def afterAll() {
    HBaseJournalInit.disableTable(config, settings.table)
    HBaseJournalInit.deleteTable(config, settings.table)

    HBaseJournalInit.disableTable(config, settings.snapshotTable)
    HBaseJournalInit.deleteTable(config, settings.snapshotTable)

    HBaseClientFactory.reset()

    super.afterAll()
  }

}
