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

object Project4 {
  def main(args: Array[String]) {
    // exit if arguments not passed as command line param.
    if (args.length < 1) {
      println("INVALID NO OF ARGS.  USAGE :")
      System.exit(1)
    } else if (args.length == 1) {
      var factor = args(0).toInt

      // create actor system and a watcher actor
      val system = ActorSystem("Twitter", ConfigFactory.load(ConfigFactory.parseString("""{ "akka" : { "actor" : { "provider" : "akka.remote.RemoteActorRefProvider" }, "remote" : { "enabled-transports" : [ "akka.remote.netty.tcp" ], "netty" : { "tcp" : { "port" : 13000 } } } } } """)))
      /* creates a watcher Actor. In the constructor, it starts joining nodes one by one to the n/w.
       * Once that is done, it starts sending messages. */
      val watcher = system.actorOf(Props(new Watcher(factor)), name = "Watcher")
    }
  }

  object Watcher {
    case class Terminate(node: Client)
  }

  class Watcher(factor: Int) extends Actor {
    import Watcher._
    import context._
    var cancellable: Cancellable = null

    // keep track of actors and application obj.
    var nodesArr = ArrayBuffer.empty[Client]

    // create nodes (actors) and asks them to join to the n/w at intervals of 10 ms. It will then wait for all of them to join.
    // add first actor immediately and the rest after a second.

    for (i <- 1 to 2) {
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

  object Client {
    case object Init
    case object FAILED
  }

  class Client extends Actor {
    import context._
    import Client._

    /* Constructor Started */

    /* Constructor Ended */

    // Receive block when in Initializing State before Node is Alive.
    def Initializing: Receive = LoggingReceive {
      case Init =>
        println("******************************************************************")
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