package akka.persistence.hbase.journal

import java.util.{ArrayList => JArrayList}
import java.{util => ju}

import akka.actor.{Actor, ActorLogging}
import akka.persistence.hbase.common.{AsyncBaseUtils, HBaseSerialization}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.util.Bytes._
import org.hbase.async.{HBaseClient, KeyValue}

// todo split into one API classes and register the impls as extensions
trait HBaseJournalBase extends HBaseSerialization with AsyncBaseUtils {
  this: Actor with ActorLogging =>

  def client: HBaseClient

  def hBasePersistenceSettings: PluginPersistenceSettings
  def hadoopConfig: Configuration

  lazy val Table = hBasePersistenceSettings.table
  lazy val TableBytes = toBytes(Table)

  type AsyncBaseRows = JArrayList[JArrayList[KeyValue]]

  val Family = hBasePersistenceSettings.family
  val FamilyBytes = toBytes(Family)

}
