package project4.src

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.DurationInt
import scala.util.Random

import com.typesafe.config.ConfigFactory

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSelection
import akka.actor.ActorSelection.toScala
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.actorRef2Scala
import akka.event.LoggingReceive

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
      val system = ActorSystem("TwitterClients", ConfigFactory.load(ConfigFactory.parseString("""{ "akka" : { "actor" : { "provider" : "akka.remote.RemoteActorRefProvider" }, "remote" : { "enabled-transports" : [ "akka.remote.netty.tcp" ], "netty" : { "tcp" : { "port" : 13000 , "maximum-frame-size" : 1280000b } } } } } """)))
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
    val router = actorSelection("akka.tcp://TwitterServer@" + ipAddress + ":12000/user/router")
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
      var node = actorOf(Props(new Client()), name = "Worker" + i)
      node ! Client.Init(TweetsPerUser(i), duration, indexes, router)
      nodesArr += node
    }

    val server = actorSelection("akka.tcp://TwitterServer@" + ipAddress + ":12000/user/Watcher")
    server ! Project4Server.Watcher.Init(nodesArr.map(_.path.name))

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

  class Event(relative: Int = 0, abs: Long = 0, tweets: Int = 0) {
    var relativeTime = relative
    var absTime = abs
    var noOfTweets = tweets
  }

  object Client {
    case class Init(avgNoOfTweets: Int, duration: Int, indexes: ArrayBuffer[Int], router: ActorSelection)
    case class FollowingList(followingList: ArrayBuffer[ActorRef])
    case class FollowersList(followingList: ArrayBuffer[ActorRef])
    case class Tweet(noOfTweets: Int)
    case object FAILED
  }

  class Client() extends Actor {
    import context._
    import Client._

    /* Constructor Started */
    var routerRef: ActorSelection = null
    var followingList = ArrayBuffer.empty[ActorRef]
    var followersList = ArrayBuffer.empty[ActorRef]
    var events = ArrayBuffer.empty[Event]
    /* Constructor Ended */

    def Initialize(avgNoOfTweets: Int, duration: Int, indexes: ArrayBuffer[Int]) {
      val pdf = new PDF()

      // Generate Timeline for tweets for given duration. Std. Deviation = Mean/4 (25%),	Mean = TweetsPerUser(i)
      var mean = avgNoOfTweets / duration.toDouble
      var tweetspersecond = pdf.gaussian.map(_ * (mean / 4) + mean).sample(duration).map(a => Math.round(a).toInt)
      var skewedRate = tweetspersecond.sortBy(a => a).takeRight(indexes.length).map(_ * 2) // double value of 10% of largest values to simulate peaks.
      for (j <- 0 to indexes.length - 1) {
        tweetspersecond(indexes(j)) = skewedRate(j)
      }

      for (j <- 0 to duration - 1) {
        events += new Event(j, 0, tweetspersecond(j))
      }
      events = events.filter(a => a.noOfTweets > 0).sortBy(a => a.relativeTime)
    }

    def setAbsoluteTime(baseTime: Long) {
      var tmp = events.size
      for (j <- 0 to tmp - 1) {
        events(j).absTime = baseTime + (events(j).relativeTime * 1000)
      }
    }

    def runEvent() {
      if (!events.isEmpty) {
        var tmp = events.head
        var relative = (tmp.absTime - System.currentTimeMillis()).toInt
        if (relative < 0) {
          relative = 0
        }
        events.trimStart(1)
        system.scheduler.scheduleOnce(relative milliseconds, self, Tweet(tmp.noOfTweets))
      }
    }

    // Receive block when in Initializing State before Node is Alive.
    def Initializing: Receive = LoggingReceive {
      case Init(avgNoOfTweets, duration, indexes, router) =>
        routerRef = router
        Initialize(avgNoOfTweets, duration, indexes)

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