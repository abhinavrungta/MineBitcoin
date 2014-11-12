package project4.src

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.DurationInt

import com.typesafe.config.ConfigFactory

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Terminated
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.actorRef2Scala
import akka.event.LoggingReceive
import akka.routing.SmallestMailboxRouter

object Project4Server {
  var nodesArr = ArrayBuffer.empty[ActorRef]
  var followersList = Array.fill(1)(ArrayBuffer.empty[ActorRef])
  var followingList = Array.fill(1)(ArrayBuffer.empty[ActorRef])
  var tweetTPS = ArrayBuffer.empty[Int]
  var tmp = 0

  def main(args: Array[String]) {
    // create an actor system.
    val system = ActorSystem("TwitterServer", ConfigFactory.load(ConfigFactory.parseString("""{ "akka" : { "actor" : { "provider" : "akka.remote.RemoteActorRefProvider" }, "remote" : { "enabled-transports" : [ "akka.remote.netty.tcp" ], "netty" : { "tcp" : { "port" : 12000 , "maximum-frame-size" : 1280000b } } } } } """)))

    // creates a watcher Actor. In the constructor, it initializes nodesArr and creates followers and following list
    val watcher = system.actorOf(Props(new Watcher()), name = "Watcher")
  }

  object Watcher {
    case class Init(nodesArr: ArrayBuffer[ActorRef])
    case object Time
  }

  class Watcher extends Actor {
    import Watcher._
    import context._

    val pdf = new PDF()
    var cancellable = system.scheduler.schedule(0 seconds, 5000 milliseconds, self, Time)
    val router = context.actorOf(Props[Server].withRouter(SmallestMailboxRouter(30)), name = "Router")
    context.watch(router)

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
      for (j <- 0 to noOfUsers - 1) {
        nodesArr(j) ! Project4Client.Client.FollowingList(followingList(j))
        nodesArr(j) ! Project4Client.Client.FollowersList(followersList(j))
      }
    }

    // Receive block for the Watcher.
    final def receive = LoggingReceive {
      case Init(arr) =>
        Initialize(arr)

      case Time =>
        tweetTPS += tmp
        tmp = 0

      case Terminated(ref) =>
        if (ref == router) {
          println(tweetTPS)
          system.shutdown
        }

      case _ => println("FAILED HERE")
    }
  }

  object Server {
    case class Tweet(userId: Int, time: Long, msg: String)
  }

  class Server extends Actor {
    import Server._
    import context._

    // Receive block for the Watcher.
    final def receive = LoggingReceive {
      case Tweet(userId, time, msg) =>
        tmp += 1

      case _ => println("FAILED HERE")
    }
  }
}