package akka.persistence.hbase.journal

import akka.persistence.journal.JournalSpec
import com.typesafe.config.ConfigFactory

/**
 * Plugin TCK (Martin's) Spec
 */
class PersistencePluginTCKSpec extends JournalSpec {

  // because of costy init of hbase-client, afterwards it's fast
  lazy val config = ConfigFactory.parseString("akka.test.timefactor=5").withFallback(ConfigFactory.load())

  override protected def beforeAll() {
    HBaseJournalInit.createTable(config)

    HBaseJournalInit.getHBaseConfig(config)
    super.beforeAll()
  }

  override protected def afterAll() {
    super.beforeAll()

    HBaseJournalInit.createTable(config)
  }

}
