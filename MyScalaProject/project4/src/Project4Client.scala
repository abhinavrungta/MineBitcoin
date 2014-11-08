package project4.src;

import java.security.MessageDigest
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.DurationInt
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.actorRef2Scala
import akka.event.LoggingReceive
import akka.actor.Cancellable
import com.typesafe.config.ConfigFactory
import com.sun.jmx.snmp.Timestamp

object Project4Client {
  def main(args: Array[String]) {
    // exit if arguments not passed as command line param.
    if (args.length < 2) {
      println("INVALID NO OF ARGS.  USAGE :")
      System.exit(1)
    } else if (args.length == 2) {
      var avgTweetsPerUser = args(0).toInt
      var noOfUsers = args(1).toInt

      // create actor system and a watcher actor
      val system = ActorSystem("TwitterClients", ConfigFactory.load(ConfigFactory.parseString("""{ "akka" : { "actor" : { "provider" : "akka.remote.RemoteActorRefProvider" }, "remote" : { "enabled-transports" : [ "akka.remote.netty.tcp" ], "netty" : { "tcp" : { "port" : 13000 } } } } } """)))
      // creates a watcher Actor. In the constructor, it starts joining nodes one by one to the n/w.
      val watcher = system.actorOf(Props(new Watcher(noOfUsers, avgTweetsPerUser)), name = "Watcher")
    }
  }

  object Watcher {
    case class Terminate(node: Client)
  }

  class Watcher(noOfUsers: Int, avgTweetsPerUser: Int) extends Actor {
    import Watcher._
    import context._

    // keep track of actors.
    var nodesArr = ArrayBuffer.empty[Client]

    for (i <- 1 to noOfUsers) {
      var node = actorOf(Props(new Client()), name = "Worker" + i)
      system.scheduler.scheduleOnce(1000 + 10 * i milliseconds, node, Client.Init)
    }

    var startTime = System.currentTimeMillis()
    // end of constructor

    // Receive block for the Watcher.
    final def receive = LoggingReceive {
      case Terminate(node) =>
        nodesArr -= node
        val finalTime = System.currentTimeMillis()
        println("No of Alive Nodes " + nodesArr.length)
        // when all actors are down, shutdown the system.
        if (nodesArr.isEmpty) {
          println("Final:" + (finalTime - startTime))
          context.system.shutdown
        }

      case _ => println("FAILED HERE")
    }
  }

  class EventNode(time: Long, event: String) {
    var timestamp = time
    var eventDesc = event
  }

  object Client {
    case object Init
    case object FAILED
  }

  class Client extends Actor {
    import context._
    import Client._

    /* Constructor Started */
    var clientId = 0
    var tweetRate = 0
    var eventsArr = ArrayBuffer.empty[EventNode]
    var duration = system.scheduler.schedule(0 seconds, 10 milliseconds, self, Init)

    /* Constructor Ended */

    def insertEvent(time: Long, event: String) {
      eventsArr += new EventNode(time, event)
      eventsArr.sortBy(a => a.timestamp)
    }

    def runEvent() {
      system.scheduler.scheduleOnce(2 milliseconds, self, Client.Init)
    }

    // Receive block when in Initializing State before Node is Alive.
    def Initializing: Receive = LoggingReceive {
      case Init =>

      case _ => println("FAILED")

    }

    // Receive block when in Alive State.
    def Alive: Receive = LoggingReceive {
      case _ =>
    }

    // default state of Actor.
    def receive = Initializing
  }
}