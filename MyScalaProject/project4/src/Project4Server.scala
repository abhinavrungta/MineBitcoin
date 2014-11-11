package project4.src;

import scala.collection.mutable.ArrayBuffer
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.actorRef2Scala
import akka.event.LoggingReceive
import com.typesafe.config.ConfigFactory
import akka.routing.SmallestMailboxRouter

object Project4Server {
  var nodesArr = ArrayBuffer.empty[ActorRef]
  var followersList = Array.fill(1)(ArrayBuffer.empty[ActorRef])
  var followingList = Array.fill(1)(ArrayBuffer.empty[ActorRef])

  def main(args: Array[String]) {
    // create actor system and a watcher actor
    val system = ActorSystem("TwitterServer", ConfigFactory.load(ConfigFactory.parseString("""{ "akka" : { "actor" : { "provider" : "akka.remote.RemoteActorRefProvider" }, "remote" : { "enabled-transports" : [ "akka.remote.netty.tcp" ], "netty" : { "tcp" : { "port" : 12000 } } } } } """)))

    // create n actors in the server for handling requests.
    val arr = ArrayBuffer.empty[ActorRef]
    for (j <- 0 to 49) {
      arr += system.actorOf(Props(new Server()), name = "Server" + j)
    }
    // create a router.
    val router: ActorRef = system.actorOf(Props.empty.withRouter(SmallestMailboxRouter(routees = arr.toVector)), name = "router")
    // creates a watcher Actor. In the constructor, it initializes nodesArr and creates followers and following list
    val watcher = system.actorOf(Props(new Watcher()), name = "Watcher")
  }

  object Watcher {
    case class Init(nodesArr: ArrayBuffer[ActorRef])
  }

  class Watcher extends Actor {
    import Watcher._
    import context._

    val pdf = new PDF()
    // keep track of actors.

    def Initialize(arr: ArrayBuffer[ActorRef]) {
      nodesArr = arr
      var noOfUsers = arr.length
      // we have data that people who tweet more have more followers. map the # of tweets to the followers.
      var FollowersPerUser = pdf.exponential(1.0 / 208.0).sample(noOfUsers).map(_.toInt)
      FollowersPerUser = FollowersPerUser.sortBy(a => a)

      // since no of followers are roughly the same as no of following.
      var FollowingPerUser = pdf.exponential(1.0 / 208.0).sample(noOfUsers).map(_.toInt)
      FollowingPerUser = FollowingPerUser.sortBy(a => a)

      followersList = Array.fill(noOfUsers)(ArrayBuffer.empty[ActorRef])
      followingList = Array.fill(noOfUsers)(ArrayBuffer.empty[ActorRef])

      for (j <- 0 to noOfUsers - 1) {
        var k = -1
        // construct list of followers.
        println(j)
        while (FollowingPerUser(j) > 0 && k < noOfUsers) {
          k += 1
          while (k < noOfUsers && FollowersPerUser(k) > 0) {
            k += 1
          }
          if (k < noOfUsers) {
            followingList(j) += nodesArr(k)
            followersList(k) += nodesArr(j)
            FollowingPerUser(j) -= 1
            FollowersPerUser(k) -= 1
          }
        }
      }

      for (j <- 0 to noOfUsers - 1) {
        nodesArr(j) ! Project4Client.Client.FollowingList(followingList(j))
        nodesArr(j) ! Project4Client.Client.FollowersList(followersList(j))
      }
    }
    // end of constructor

    // Receive block for the Watcher.
    final def receive = LoggingReceive {
      case Init(arr) =>
        Initialize(arr)

      case _ => println("FAILED HERE")
    }
  }

  object Server {
    case object Init
  }

  class Server extends Actor {
    import Server._
    import context._

    // Receive block for the Watcher.
    final def receive = LoggingReceive {
      case Init =>

      case _ => println("FAILED HERE")
    }
  }
}