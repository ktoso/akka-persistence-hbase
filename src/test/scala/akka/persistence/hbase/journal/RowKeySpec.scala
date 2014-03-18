package akka.persistence.hbase.journal

import akka.persistence.hbase.common.RowKey
import org.scalatest.{FlatSpec, Matchers}

class RowKeySpec extends FlatSpec with Matchers {

  behavior of "RowKey"

  implicit val journalConfig = new PersistencePluginSettings(null, null, null, null, 50, 1, null, null, false, null, null, null, null)

  it should "find first key in partition" in {
    val rowKeys = for {
      p <- 1 to 50
    } yield RowKey.firstInPartition("x", p)

    val keys = rowKeys.map(_.toKeyString)

//    keys foreach { k => info("key: " + k.toKeyString) }
    keys should contain ("004-x-00000000000000000004")
    keys should contain ("050-x-00000000000000000050")

    rowKeys.map(_.part) should equal ((1 to 50).toList)
  }

  it should "find first key in partition, with lower bound 4" in {
    val rowKeys = for {
      p <- 1 to 50
    } yield RowKey.firstInPartition("x", p, fromSequenceNr = 4)

    val keys = rowKeys.map(_.toKeyString)

//    keys foreach { k => info("key: " + k.toKeyString) }
    keys should contain ("001-x-00000000000000000004")
    keys should contain ("002-x-00000000000000000004")
    keys should contain ("003-x-00000000000000000004")
    keys should contain ("004-x-00000000000000000004")
    keys should contain ("005-x-00000000000000000005")
    keys should contain ("050-x-00000000000000000050")

    rowKeys.map(_.part) should equal ((1 to 50).toList)
  }

  it should "find last key in partition" in {
    val rowKeys = for {
      p <- 1 to 50
    } yield RowKey.lastInPartition("x", p)

    val keys = rowKeys.map(_.toKeyString)

//    rowKeys foreach { k => info("key: " + k.toKeyString) }
    keys should contain ("001-x-09223372036854775807")
    keys should contain ("002-x-09223372036854775806")
    keys should contain ("050-x-09223372036854775800")

    rowKeys.map(_.part) should equal ((1 to 50).toList)
  }

  it should "find last key in partition, with upper bound 7" in {
    val rowKeys = for {
      p <- 1 to 50
    } yield RowKey.lastInPartition("x", p, toSequenceNr = 7)

    val keys = rowKeys.map(_.toKeyString)

//    rowKeys foreach { k => info("key: " + k.toKeyString) }
    keys should contain ("001-x-00000000000000000007")
    keys should contain ("002-x-00000000000000000007")
    keys should contain ("050-x-00000000000000000007")

    rowKeys.map(_.part) should equal ((1 to 50).toList)
  }

}
