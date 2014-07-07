package akka.persistence.hbase.journal

import java.util.{ArrayList => JArrayList}
import java.{util => ju}

import akka.actor.{Actor, ActorLogging}
import akka.persistence.hbase.common.{AsyncBaseUtils, HBaseSerialization, HBaseUtils}
import org.apache.hadoop.conf.Configuration
import org.hbase.async.{HBaseClient, KeyValue}

// todo split into one API classes and register the impls as extensions
trait HBaseJournalBase extends HBaseSerialization
  with HBaseUtils with AsyncBaseUtils {
  this: Actor with ActorLogging =>

  def client: HBaseClient

  def hBasePersistenceSettings: PersistencePluginSettings
  def hadoopConfig: Configuration

  type AsyncBaseRows = JArrayList[JArrayList[KeyValue]]

}
