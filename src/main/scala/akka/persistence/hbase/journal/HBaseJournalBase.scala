package akka.persistence.hbase.journal

import org.apache.hadoop.hbase.util.Bytes
import HBaseJournalInit._
import akka.actor.{Actor, ActorLogging}
import org.hbase.async.{HBaseClient, PutRequest, DeleteRequest, KeyValue}
import java.util. { ArrayList => JArrayList }
import scala.collection.mutable
import java.{ util => ju }
import com.typesafe.config.Config
import org.apache.hadoop.hbase.util.Bytes._
import scala.concurrent.Future
import scala.Array
import akka.persistence.hbase.common.{AsyncBaseUtils, Columns, DeferredConversions, HBaseSerialization}
import org.apache.hadoop.conf.Configuration

// todo split into one API classes and register the impls as extensions
trait HBaseJournalBase extends HBaseSerialization with AsyncBaseUtils {
  this: Actor with ActorLogging =>

  def client: HBaseClient

  def hBasePersistenceSettings: HBasePersistenceSettings
  def hadoopConfig: Configuration

  lazy val Table = hBasePersistenceSettings.table
  lazy val TableBytes = toBytes(Table)

  type AsyncBaseRows = JArrayList[JArrayList[KeyValue]]

  /** Used to avoid writing all data to the same region - see "hot region" problem */
  def partition(sequenceNr: Long): Long = sequenceNr % hBasePersistenceSettings.partitionCount

  val Family = hBasePersistenceSettings.family
  val FamilyBytes = toBytes(Family)

}
