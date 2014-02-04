package akka.contrib.persistence.hbase.journal

import akka.persistence._
import org.apache.hadoop.hbase.util.Bytes
import akka.serialization.Serialization
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
import akka.contrib.persistence.hbase.common.{DeferredConversions, HBaseSerialization}

// todo split into one API classes and register the impls as extensions
trait HBaseJournalBase extends HBaseSerialization
  with DeferredConversions with PersistenceMarkers {
  this: Actor with ActorLogging =>

  /** hbase-journal configuration */
  def config: Config

  def client: HBaseClient

  lazy val journalConfig = HBaseJournalConfig(config)
  lazy val hadoopConfig = getHBaseConfig(config)

  lazy val Table = config.getString("messages-table")
  lazy val TableBytes = toBytes(Table)

  type AsyncBaseRows = JArrayList[JArrayList[KeyValue]]

  /** Used to avoid writing all data to the same region - see "hot region" problem */
  def partition(sequenceNr: Long): Long = sequenceNr % journalConfig.partitionCount

  @inline def padded(l: Long, howLong: Int) =
    String.valueOf(l).reverse.padTo(howLong, "0").reverse.mkString

  case class RowKey(processorId: String, sequenceNr: Long) {
    val part = partition(sequenceNr)
    val toBytes = Bytes.toBytes(toKeyString)
    def toKeyString = s"${padded(part, 3)}-$processorId-${padded(sequenceNr, 20)}"
  }
  object RowKey {
    /**
     * Since we're salting (prefixing) the entries with partition numbers,
     * we must use this pattern for scanning for "all messages for processorX"
     */
    def patternForProcessor(processorId: String) = s""".*-$processorId-.*"""

    /** First key possible, similar to: `0-id-000000000000000000000`*/
    def firstForProcessor(processorId: String) =
      RowKey(processorId, 0)

    /** Last key possible, similar to: `999-id-Long.MaxValue`*/
    def lastForProcessor(processorId: String) =
      RowKey(processorId, 0)
  }

  object Columns {
    val Family = toBytes(config.getString("family"))

    val ProcessorId = toBytes("processorId")
    val SequenceNr  = toBytes("sequenceNr")
    val Marker      = toBytes("marker")
    val Message     = toBytes("payload")
  }
  import Columns._

  protected def findColumn(columns: mutable.Buffer[KeyValue], qualifier: Array[Byte]) =
    columns find { kv =>
      ju.Arrays.equals(kv.qualifier, qualifier)
    } getOrElse {
      throw new RuntimeException(s"Unable to find [${Bytes.toString(qualifier)}}] field from: ${columns.map(kv => Bytes.toString(kv.qualifier))}")
    }

  protected def deleteRow(key: Array[Byte]): Future[Unit] = {
    log.debug(s"Permanently deleting row: ${Bytes.toString(key)}")
    executeDelete(key)
  }

  protected def markRowAsDeleted(key: Array[Byte]): Future[Unit] = {
    log.debug(s"Marking as deleted, for row: ${Bytes.toString(key)}")
    executePut(key, Array(Marker), Array(DeletedMarkerBytes))
  }

  protected def executeDelete(key: Array[Byte]): Future[Unit] = {
    val request = new DeleteRequest(TableBytes, key)
    client.delete(request)
  }

  protected def executePut(key: Array[Byte], qualifiers: Array[Array[Byte]], values: Array[Array[Byte]]): Future[Unit] = {
    val request = new PutRequest(TableBytes, key, Family, qualifiers, values)
    client.put(request)
  }

  /**
   * Sends the buffered commands to HBase. Does not guarantee that they "complete" right away.
   */
  def flushWrites() {
    client.flush()
  }

  protected def newScanner() = {
    val scanner = client.newScanner(Table)
    scanner.setFamily(Family)
    scanner
  }

}
