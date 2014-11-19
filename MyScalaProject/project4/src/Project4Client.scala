package project4.src

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.DurationInt
import scala.util.Random
import com.typesafe.config.ConfigFactory
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSelection
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.actorRef2Scala
import akka.event.LoggingReceive
import akka.actor.PoisonPill
import akka.actor.Cancellable

object Project4Client {
  def main(args: Array[String]) {
    // exit if arguments not passed as command line param.
    if (args.length < 3) {
      println("INVALID NO OF ARGS.  USAGE :")
      System.exit(1)
    } else if (args.length == 3) {
      var avgTweetsPerSecond = args(0).toInt
      var noOfUsers = args(1).toInt
      var ipAddress = args(2)

      // create actor system and a watcher actor.
      val system = ActorSystem("TwitterClients", ConfigFactory.load(ConfigFactory.parseString("""{ "akka" : { "actor" : { "provider" : "akka.remote.RemoteActorRefProvider" }, "remote" : { "enabled-transports" : [ "akka.remote.netty.tcp" ], "netty" : { "tcp" : { "port" : 13000 , "maximum-frame-size" : 1280000b } } } } } """)))
      // creates a watcher Actor.
      val watcher = system.actorOf(Props(new Watcher(noOfUsers, avgTweetsPerSecond, ipAddress)), name = "Watcher")
    }
  }
  object Watcher {
    case class Terminate(node: ActorRef)
  }

  class Watcher(noOfUsers: Int, avgTweetsPerSecond: Int, ipAddress: String) extends Actor {
    import context._
    import Watcher._

    var ctr = 0
    val pdf = new PDF()
    // select router.
    //val router = actorSelection("akka.tcp://TwitterServer@" + ipAddress + ":12000/user/Watcher/Router")
    val router = actorFor("akka.tcp://TwitterServer@" + ipAddress + ":12000/user/Watcher/Router")

    // 307 => mean (avg tweets per user).	sample(size) => size is the no of Users.
    var TweetsPerUser = pdf.exponential(1.0 / 307.0).sample(noOfUsers).map(_.toInt)
    TweetsPerUser = TweetsPerUser.sortBy(a => a)

    // calculate duration required to produce given tweets at given rate.
    var duration = TweetsPerUser.sum / avgTweetsPerSecond
    println(duration)

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

    // keep track of Client Actors.
    var nodesArr = ArrayBuffer.empty[ActorRef]
    // start running after 10 seconds from currentTime.
    var absoluteStartTime = System.currentTimeMillis() + (10 * 1000)
    // create given number of clients and initialize.
    for (i <- 0 to noOfUsers - 1) {
      var node = actorOf(Props(new Client()), name = "" + i)
      // Initialize Clients with info like #Tweets, duration of tweets, start Time, router address. 
      node ! Client.Init(TweetsPerUser(i), duration, indexes, absoluteStartTime, router)
      nodesArr += node
      context.watch(node)
    }

    // Initiate Server with list of Users.
    val server = actorSelection("akka.tcp://TwitterServer@" + ipAddress + ":12000/user/Watcher")
    server ! Project4Server.Watcher.Init(nodesArr)

    var startTime = System.currentTimeMillis()
    // end of constructor

    // Receive block for the Watcher.
    final def receive = LoggingReceive {
      case Terminate(node) =>
        ctr += 1
        val finalTime = System.currentTimeMillis()
        //println("No of Dead Nodes " + ctr)
        // when all actors are down, shutdown the system.
        if (nodesArr.length == ctr) {
          println("Final:" + (finalTime - startTime))
          for (i <- 0 to noOfUsers - 1) {
            nodesArr(i) ! Client.Stop
          }
          router ! PoisonPill
          context.system.shutdown
        }

      case _ => println("FAILED HERE")
    }
  }

  class Event(relative: Int = 0, tweets: Int = 0) {
    var relativeTime = relative
    var absTime: Long = 0
    var noOfTweets = tweets
  }

  object Client {
    case class Init(avgNoOfTweets: Int, duration: Int, indexes: ArrayBuffer[Int], absoluteTime: Long, router: ActorRef)
    case class FollowingList(followingList: ArrayBuffer[ActorRef])
    case class FollowersList(followingList: ArrayBuffer[ActorRef])
    case class Tweet(noOfTweets: Int)
    case class RecvTimeline(tweets: Map[Int, String])
    case object Stop
  }

  class Client() extends Actor {
    import context._
    import Client._

    /* Constructor Started */
    var routerRef: ActorRef = null
    var followingList = ArrayBuffer.empty[ActorRef]
    var followersList = ArrayBuffer.empty[ActorRef]
    var events = ArrayBuffer.empty[Event]
    var id = self.path.name.toInt
    val rand = new Random()
    var cancellable: Cancellable = null
    var ctr = 0
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
        events += new Event(j, tweetspersecond(j))
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
      } else {
        parent ! Watcher.Terminate(self)
      }
    }

    def generateTweet(): String = {
      var length = rand.nextInt(140)
      var chars = "abcdefghjkmnpqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ0123456789"
      var size = chars.size
      var randomString = ""
      for (j <- 1 to length) {
        randomString += chars(rand.nextInt(size))
      }
      return randomString
    }

    // Receive block when in Initializing State before Node is Alive.
    final def receive = LoggingReceive {
      case Init(avgNoOfTweets, duration, indexes, absoluteTime, router) =>
        routerRef = router
        Initialize(avgNoOfTweets, duration, indexes)
        setAbsoluteTime(absoluteTime)
        var relative = (absoluteTime - System.currentTimeMillis()).toInt
        cancellable = system.scheduler.schedule(relative milliseconds, 3 second, routerRef, Project4Server.Server.SendTimeline(id))
        runEvent()

      case FollowingList(arr) =>
        followingList = arr

      case FollowersList(arr) =>
        followersList = arr

      case Tweet(noOfTweets: Int) =>
        for (j <- 1 to noOfTweets) {
          routerRef ! Project4Server.Server.Tweet(id, System.currentTimeMillis(), generateTweet())
        }
        runEvent()

      case RecvTimeline(map) =>
        ctr += map.size

      case Stop =>
        cancellable.cancel
        context.stop(self)

      case _ => println("FAILED")

    }
  }
}
