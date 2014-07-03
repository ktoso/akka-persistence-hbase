package akka.persistence.hbase.journal

import akka.persistence.journal.JournalSpec
import com.typesafe.config.ConfigFactory

/**
 * Plugin TCK (Martin's) Spec
 */
class PersistenceTestKitSpec extends JournalSpec {

  lazy val config = ConfigFactory.load()

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
