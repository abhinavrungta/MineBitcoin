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
import scala.util.Random

object Project4Client {
  def main(args: Array[String]) {
    // exit if arguments not passed as command line param.
    if (args.length < 2) {
      println("INVALID NO OF ARGS.  USAGE :")
      System.exit(1)
    } else if (args.length == 3) {
      var avgTweetsPerSecond = args(0).toInt
      var noOfUsers = args(1).toInt
      var ipAddress = args(2)

      // create actor system and a watcher actor
      val system = ActorSystem("TwitterClients", ConfigFactory.load(ConfigFactory.parseString("""{ "akka" : { "actor" : { "provider" : "akka.remote.RemoteActorRefProvider" }, "remote" : { "enabled-transports" : [ "akka.remote.netty.tcp" ], "netty" : { "tcp" : { "port" : 13000 } } } } } """)))
      // creates a watcher Actor. In the constructor, it starts joining nodes one by one to the n/w.
      val watcher = system.actorOf(Props(new Watcher(noOfUsers, avgTweetsPerSecond, ipAddress)), name = "Watcher")
    }
  }

  object Watcher {
    case class Terminate(node: ActorRef)
  }

  class Watcher(noOfUsers: Int, avgTweetsPerSecond: Int, ipAddress: String) extends Actor {
    import Watcher._
    import context._

    val pdf = new PDF()
    // keep track of actors.
    var nodesArr = ArrayBuffer.empty[ActorRef]

    // 307 => mean (avg tweets per user).	sample(size) => size is the no of Users.
    var TweetsPerUser = pdf.exponential(1.0 / 307.0).sample(noOfUsers).map(_.toInt)
    TweetsPerUser = TweetsPerUser.sortBy(a => a)

    // calculate duration required to produce given tweets at given rate.
    var duration = TweetsPerUser.sum / avgTweetsPerSecond

    // get times for 5% of duration. Duration is relative -> 1 to N
    var percent5 = (duration * 0.05).toInt
    var indexes = ArrayBuffer.empty[Int]
    val rnd = new Random
    for (i <- 1 to percent5) {
      var tmp = rnd.nextInt(duration)
      while (indexes.contains(tmp)) {
        tmp = rnd.nextInt(duration)
      }
      indexes += tmp
    }

    // create given number of clients and initialize.
    for (i <- 0 to noOfUsers - 1) {
      var node = actorOf(Props(new Client(TweetsPerUser(i), duration, indexes)), name = "Worker" + i)
      nodesArr += node
    }

    val server = actorSelection("akka.tcp://TwitterServer@" + ipAddress + ":12000/user/Master")
    server ! Project4Server.Watcher.Init(nodesArr)

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
    case class FollowingList(followingList: ArrayBuffer[ActorRef])
    case class FollowersList(followingList: ArrayBuffer[ActorRef])
  }

  class Client(avgNoOfTweets: Int, duration: Int, indexes: ArrayBuffer[Int]) extends Actor {
    import context._
    import Client._

    /* Constructor Started */
    var followingList = ArrayBuffer.empty[ActorRef]
    var followersList = ArrayBuffer.empty[ActorRef]

    val pdf = new PDF()

    // Generate Timeline for tweets for given duration. Std. Deviation = Mean/4 (25%),	Mean = TweetsPerUser(i)
    var mean = avgNoOfTweets / duration.toDouble
    var tweetspersecond = pdf.gaussian.map(_ * (mean / 4) + mean).sample(duration).map(a => Math.round(a).toInt)
    var skewedRate = tweetspersecond.sortBy(a => a).takeRight(indexes.length).map(_ * 2) // double value of 10% of largest values to simulate peaks.
    for (j <- 0 to indexes.length - 1) {
      tweetspersecond(indexes(j)) = skewedRate(j)
    }

    /* Constructor Ended */

    def runEvent() {
      system.scheduler.scheduleOnce(2 milliseconds, self, Client.Init)
    }

    // Receive block when in Initializing State before Node is Alive.
    def Initializing: Receive = LoggingReceive {
      case Init =>

      case FollowingList(arr) =>
        followingList = arr

      case FollowersList(arr) =>
        followersList = arr

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