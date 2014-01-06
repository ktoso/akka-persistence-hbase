package akka.persistence.journal.hbase.example

import akka.actor.{Props, ActorSystem}
import akka.persistence.{Persistent, Processor}

class PingActor extends Processor {
  def receive = {
    case Persistent(msg, seqNr) =>
      sender ! s"ping-$seqNr"
  }
}

class PongActor extends Processor {
  def receive = {
    case Persistent(msg, seqNr) =>
      sender ! s"pong-$seqNr"
  }
}

object JournaledPingPongApp extends App {

  val num = readLine("How many pairs of ping/pong actors should we start? ").toInt

  val system = ActorSystem("ping-pong-system")

  (0 to num) foreach { i =>
    val pinger = system.actorOf(Props[PingActor], s"pinger-$i")
    val ponger = system.actorOf(Props[PingActor], s"ponger-$i")
    ponger.tell("ping", pinger)

    println(s"Started ${i}th pair...")
  }

  readLine("Hit enter to quit.")

  system.shutdown()
}
