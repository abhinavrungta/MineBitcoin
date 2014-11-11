package project4.src

import scala.collection.mutable.ArrayBuffer
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.actorRef2Scala
import akka.event.LoggingReceive
import com.typesafe.config.ConfigFactory
import akka.routing.SmallestMailboxRouter
import akka.routing.RoundRobinRouter

object Project4Server {
  var nodesArr = ArrayBuffer.empty[ActorRef]
  var followersList = Array.fill(1)(ArrayBuffer.empty[ActorRef])
  var followingList = Array.fill(1)(ArrayBuffer.empty[ActorRef])

  def main(args: Array[String]) {
    // create an actor system.
    val system = ActorSystem("TwitterServer", ConfigFactory.load(ConfigFactory.parseString("""{ "akka" : { "actor" : { "provider" : "akka.remote.RemoteActorRefProvider" }, "remote" : { "enabled-transports" : [ "akka.remote.netty.tcp" ], "netty" : { "tcp" : { "port" : 12000 , "maximum-frame-size" : 1280000b } } } } } """)))

    // create n actors in the server for handling requests.
    val arr = ArrayBuffer.empty[ActorRef]
    for (j <- 0 to 49) {
      arr += system.actorOf(Props(new Server()), name = "Server" + j)
    }
    // create a router with given actors.
    val router = system.actorOf(Props.empty.withRouter(SmallestMailboxRouter(routees = arr.toVector)), name = "Router")
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
      // we have data that people who tweet more have more followers.
      // create a distribution for followers per user.
      var FollowersPerUser = pdf.exponential(1.0 / 208.0).sample(noOfUsers).map(_.toInt)
      FollowersPerUser = FollowersPerUser.sortBy(a => a)

      // create a distribution for followings per user.
      var FollowingPerUser = pdf.exponential(1.0 / 208.0).sample(noOfUsers).map(_.toInt)
      FollowingPerUser = FollowingPerUser.sortBy(a => a)

      // create a list for n users.
      followersList = Array.fill(noOfUsers)(ArrayBuffer.empty[ActorRef])
      followingList = Array.fill(noOfUsers)(ArrayBuffer.empty[ActorRef])

      // assign followers to each user.
      for (j <- 0 to noOfUsers - 1) {
        var k = -1
        // construct list of followers.
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

      // send followers and following list to each user.
      println("sending list.")
      for (j <- 0 to noOfUsers - 1) {
        nodesArr(j) ! Project4Client.Client.FollowingList(followingList(j))
        nodesArr(j) ! Project4Client.Client.FollowersList(followersList(j))
      }
    }

    // Receive block for the Watcher.
    final def receive = LoggingReceive {
      case Init(arr) =>
        Initialize(arr)

      case _ => println("FAILED HERE")
    }
  }

  object Server {
    case class Tweet(userId: Int, time: Long, msg: String)
  }

  class Server extends Actor {
    import Server._
    import context._

    var serverId = self.path.name

    // Receive block for the Watcher.
    final def receive = LoggingReceive {
      case Tweet(userId, time, msg) =>
        println("Recd " + msg + " in " + serverId)

      case _ => println("FAILED HERE")
    }
  }
}