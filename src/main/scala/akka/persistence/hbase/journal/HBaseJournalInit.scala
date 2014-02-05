package akka.persistence.hbase.journal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import com.typesafe.config._
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor}

object HBaseJournalInit {

  import Bytes._
  import collection.JavaConverters._

  /**
   * Creates (or adds column family to existing table) table to be used with HBaseJournal.
   *
   * @return true if a modification was run on hbase (table created or family added)
   */
  def createTable(config: Config): Boolean = {
    val conf = getHBaseConfig(config)
    val admin = new HBaseAdmin(conf)

    val journalConfig = config.getConfig("hbase-journal")
    val table = journalConfig.getString("table")
    val familyName = journalConfig.getString("family")

    try doInitTable(admin, table, familyName) finally admin.close()
  }

  private def doInitTable(admin: HBaseAdmin, tableName: String, familyName: String): Boolean = {
    if (admin.tableExists(tableName)) {
      val tableDesc = admin.getTableDescriptor(toBytes(tableName))
      if (tableDesc.getFamily(toBytes(familyName)) == null) {
        // target family does not exists, will add it.
        admin.addColumn(familyName, new HColumnDescriptor(familyName))
        true
      } else {
        // existing table is OK, no modifications run.
        false
      }
    } else {
      val tableDesc = new HTableDescriptor(toBytes(tableName))
      tableDesc.addFamily(new HColumnDescriptor(familyName))

      admin.createTable(tableDesc)
      true
    }
  }
  

  /**
   * Construct Configuration, passing in all `hbase.*` keys from the typesafe Config.
   */
  def getHBaseConfig(config: Config): Configuration = {
    val c = new Configuration()
    @inline def hbaseKey(s: String) = "hbase." + s

    val journalConfig = config.getConfig("hbase-journal")
    val hbaseConfig = journalConfig.getConfig("hbase")

    // todo does not cover all cases
    hbaseConfig.entrySet().asScala foreach { e =>
      c.set(hbaseKey(e.getKey), e.getValue.unwrapped.toString)
    }
    c
  }
}