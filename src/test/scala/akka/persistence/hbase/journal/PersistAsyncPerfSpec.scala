package akka.persistence.hbase.journal

import java.util.Date
import java.util.concurrent.TimeUnit

import akka.actor.{ActorLogging, ActorRef, ActorSystem, Props}
import akka.persistence._
import akka.testkit.{ImplicitSender, TestKit}
import com.google.common.base.Stopwatch
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.duration._

object PersistAsyncPerfSpec {

  class Writer(untilSeqNr: Long, override val persistenceId: String) extends PersistentActor
    with ActorLogging {

    var lastPersisted: Any = _

    def receiveCommand = {
      case "ask" =>
        log.info("Replying with last persisted message {}", lastPersisted)
        sender() ! lastPersisted

      case "delete" =>
        log.info("Deleting messages in {}, until {}", persistenceId, lastSequenceNr)
        deleteMessages(toSequenceNr = lastSequenceNr)

      case "boom" =>
        throw new RuntimeException("Boom!")

      case payload =>
        persistAsync(payload)(handlePersisted)
    }

    def handlePersisted(p: Any): Unit = {
      log.debug(s"persisted: {} @ {}", p, lastSequenceNr)
      if (!recoveryRunning)
        sender() ! s"p-$p"
      
      lastPersisted = p
    }

    override def receiveRecover: Receive = {
      case m => log.info("recover: " + m)
    }
  }

}

//@DoNotDiscover
class PersistAsyncPerfSpec extends TestKit(ActorSystem("test")) with FlatSpecLike
  with ImplicitSender with Matchers with BeforeAndAfterAll {

  import akka.persistence.hbase.journal.PersistAsyncPerfSpec._

  val config = system.settings.config

  behavior of "HBaseJournal"

  val messagesNr = 5000

  val messages = (1 to messagesNr) map { i => s"hello-$i-(${new Date})" }
  
  var actor = createActor(messagesNr, "w-1")

  override def beforeAll() {
    HBaseJournalInit.createTable(config)
    super.beforeAll()
  }
  
  override def afterAll() {
    system.shutdown()
    system.awaitTermination(1.minute)
    super.afterAll()
  }

  it should s"write $messagesNr messages" in {
    val stopwatch = (new Stopwatch).start()
    
    messages foreach { m =>
      println(s"actor ! $m")
      actor ! m
    }

    messagesNr.times { n => expectMsgType[String](max = 1.minute) should startWith (s"p-hello-$n") }
    stopwatch.stop()

    info(s"Sending/persisting $messagesNr messages took: $stopwatch time")
    info(s"This is ${messagesNr / stopwatch.elapsedTime(TimeUnit.SECONDS)} msg/s")

    println("=== DONE === ")

    actor ! "delete"
  }

//  it should "replay those messages" in {
//    val replayed = createActor(messagesNr, "w-1")
//
//    replayed ! "ask"
//
//    val last = expectMsgType[String]
//    last should startWith ("hello-1000")
//  }
//
//  it should "delete all messages up until that seq number" in {
//    val replayed = createActor(messagesNr, "w-1")
//
//    replayed ! "delete"
//
//    Thread.sleep(1000)
//  }

  private def createActor(awaitMessages: Long, name: String): ActorRef =
    system.actorOf(Props(classOf[Writer], awaitMessages, name))

  implicit class TimesInt(i: Int) {
    def times(block: Int => Unit) = {
      1 to i foreach block
    }
  }

  def timed(block: => Unit): Stopwatch = {
    val stopwatch = (new Stopwatch).start()
    block
    stopwatch.stop()
  }
}
