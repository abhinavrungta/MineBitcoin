import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.mutable.ParArray
import scala.concurrent.duration.DurationInt

import com.typesafe.config.ConfigFactory

import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.Terminated
import akka.actor.actorRef2Scala
import akka.event.LoggingReceive
import akka.routing.SmallestMailboxPool

object Project4Server {
  class Tweets(tid: String, id: Int, tweet: String, time: Long) {
    var AuthorId = id
    var msg = tweet
    var timeStamp: Long = time
    var tweetId = tid
  }
  var followersList = Array.fill(1)(ParArray.empty[Int])
  var followingList = Array.fill(1)(ParArray.empty[Int])
  var tweetTPS = ArrayBuffer.empty[Int]
  var ctr: AtomicInteger = new AtomicInteger()
  var rdctr: AtomicInteger = new AtomicInteger()

  var timeLines = Array.fill(1)(new java.util.concurrent.ConcurrentLinkedQueue[Tweets])

  def main(args: Array[String]) {
    // create an actor system.
    val system = ActorSystem("TwitterServer", ConfigFactory.load(ConfigFactory.parseString("""{ "akka" : { "actor" : { "provider" : "akka.remote.RemoteActorRefProvider" }, "remote" : { "enabled-transports" : [ "akka.remote.netty.tcp" ], "netty" : { "tcp" : { "port" : 12000 , "maximum-frame-size" : 1280000b } } } } } """)))

    // creates a watcher Actor. In the constructor, it initializes nodesArr and creates followers and following list
    val watcher = system.actorOf(Props(new Watcher()), name = "Watcher")
  }

  object Watcher {
    case class Init(noOfUsers: Int)
    case object Time
  }

  class Watcher extends Actor {
    import Watcher._
    import context._

    val pdf = new PDF()
    // scheduler to count no. of tweets every 5 seconds.
    var cancellable = system.scheduler.schedule(0 seconds, 5000 milliseconds, self, Time)

    // Start a router with 30 Actors in the Server.
    var cores = (Runtime.getRuntime().availableProcessors() * 1.5).toInt
    val router = context.actorOf(Props[Server].withRouter(SmallestMailboxPool(cores)), name = "Router")
    // Watch the router. It calls the terminate sequence when router is terminated.
    context.watch(router)

    def Initialize(noOfUsers: Int) {
      // create a distribution for followers per user.
      var FollowersPerUser = pdf.exponential(1.0 / 208.0).sample(noOfUsers).map(_.toInt)
      FollowersPerUser = FollowersPerUser.sortBy(a => -a)
      // create a distribution for followings per user.
      var FollowingPerUser = pdf.exponential(1.0 / 238.0).sample(noOfUsers).map(_.toInt)
      FollowingPerUser = FollowingPerUser.sortBy(a => a)

      // create a list for n users.
      var followers = Array.fill(noOfUsers)(ArrayBuffer.empty[Int])
      var following = Array.fill(noOfUsers)(ArrayBuffer.empty[Int])

      // assign followers to each user.
      for (j <- 0 to noOfUsers - 1) {
        var k = -1
        // construct list of followers.
        while (FollowingPerUser(j) > 0 && k < noOfUsers) {
          k += 1
          if (k < noOfUsers && FollowersPerUser(k) > 0) {
            following(j) += k
            followers(k) += j
            FollowingPerUser(j) -= 1
            FollowersPerUser(k) -= 1
          }
        }
      }

      followersList = Array.fill(noOfUsers)(scala.collection.parallel.mutable.ParArray.empty[Int])
      followingList = Array.fill(noOfUsers)(scala.collection.parallel.mutable.ParArray.empty[Int])
      for (j <- 0 to noOfUsers - 1) {
        followersList(j) = followers(j).toArray.par
        followingList(j) = following(j).toArray.par
      }

      // initialize timelines data.
      timeLines = Array.fill(noOfUsers)(new java.util.concurrent.ConcurrentLinkedQueue[Tweets]())
      println("Server started")
    }

    // Receive block for the Watcher.
    final def receive = LoggingReceive {
      case Init(noOfUsers) =>
        Initialize(noOfUsers)
        System.gc()

      case Time =>
        var tmp = ctr.get() - tweetTPS.sum
        tweetTPS += (tmp)
        println(tmp)

      case Terminated(ref) =>
        if (ref == router) {
          println(tweetTPS)
          println(rdctr.get())
          for (j <- 0 to followersList.size - 1) {
            //            println(timeLines(j).size)
          }
          system.shutdown
        }

      case _ => println("FAILED HERE")
    }
  }

  object Server {
    case class AddTweet(userId: Int, time: Long, msg: String)
    case class SendTimeline(userId: Int)
  }

  class Server extends Actor {
    import Server._
    import context._

    // Receive block for the Server.
    final def receive = LoggingReceive {
      case AddTweet(userId, time, msg) =>
        var tweetId = ctr.addAndGet(1) // generate tweetId.
        var tmp = new Tweets(tweetId.toString, userId, msg, time)

        var followers = followersList(userId) // get all followers and add tweet to their timeline
        followers.foreach(a => { timeLines(a).add(tmp); if (timeLines(a).size() > 100) { timeLines(a).remove() } })
        timeLines(userId).add(tmp) // add to self timeline also

      case SendTimeline(userId) =>
        rdctr.addAndGet(1)
        var tweetIds = timeLines(userId)
        var tmp: Map[String, String] = Map()
        if (!tweetIds.isEmpty) {
          var itr = tweetIds.iterator()
          while (itr.hasNext()) {
            tmp += (itr.next().tweetId -> "OK")
          }
        }
        tmp += ("0" -> "OOO")
        sender ! tmp

      case _ => println("FAILED HERE 2")
    }
  }
}
