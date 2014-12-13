import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer
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
  case class sample(id: Int, name: String, statusCount: Int, favoritesCount: Int, followersCount: Int, followingCount: Int) extends java.io.Serializable

  class User(id: Int) {
    var userId = id
    var userName = "User" + id
    var homeTimeline: ConcurrentLinkedQueue[String] = new ConcurrentLinkedQueue()
    var userTimeline: ConcurrentLinkedQueue[String] = new ConcurrentLinkedQueue()
    var favorites: ConcurrentLinkedQueue[String] = new ConcurrentLinkedQueue()
    var followers: CopyOnWriteArrayList[Int] = new CopyOnWriteArrayList()
    var following: CopyOnWriteArrayList[Int] = new CopyOnWriteArrayList()
  }

  class Tweets(tid: String, id: Int, tweet: String, time: Long, mentionsList: ArrayBuffer[String] = ArrayBuffer.empty, tags: ArrayBuffer[String] = ArrayBuffer.empty) extends java.io.Serializable {
    var authorId = id
    var message = tweet
    var timeStamp: Long = time
    var tweetId = tid
    var mentions = mentionsList
    var hashtags = tags
  }

  var tweetTPS = ArrayBuffer.empty[Int]
  var ctr: AtomicInteger = new AtomicInteger()

  var users: CopyOnWriteArrayList[User] = new CopyOnWriteArrayList()
  var tweetStore: ConcurrentHashMap[String, Tweets] = new ConcurrentHashMap()

  def main(args: Array[String]) {
    // create an actor system.
    val system = ActorSystem("TwitterServer", ConfigFactory.load(ConfigFactory.parseString("""{ "akka" : { "actor" : { "provider" : "akka.remote.RemoteActorRefProvider" }, "remote" : { "enabled-transports" : [ "akka.remote.netty.tcp" ], "netty" : { "tcp" : { "port" : 12000 , "maximum-frame-size" : 12800000b } } } } } """)))

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

      for (i <- 0 to noOfUsers - 1) {
        users.add(i, new User(i))
      }

      // assign followers to each user.
      for (j <- 0 to noOfUsers - 1) {
        var k = -1
        // construct list of followers.
        while (FollowingPerUser(j) > 0 && k < noOfUsers) {
          k += 1
          if (k < noOfUsers && FollowersPerUser(k) > 0) {
            users.get(j).following.add(k)
            users.get(k).followers.add(j)
            FollowingPerUser(j) -= 1
            FollowersPerUser(k) -= 1
          }
        }
      }
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
          system.shutdown
        }

      case _ => println("FAILED HERE")
    }
  }

  object Server {
    case class AddTweet(userId: Int, time: Long, msg: String)
    case class SendHomeTimeline(userId: Int)
    case class SendUserTimeline(userId: Int)
    case class SendFollowers(userId: Int)
    case class SendFollowing(userId: Int)
    case class SendMentions(userId: Int)
    case class SendUserProfile(userId: Int)
  }

  class Server extends Actor {
    import Server._
    import context._

    // Receive block for the Server.
    final def receive = LoggingReceive {
      case AddTweet(userId, time, msg) =>
        var regexMentions = "@[a-zA-Z0-9]+\\s*".r
        var regexTags = "#[a-zA-Z0-9]+\\s*".r
        var tweetId = ctr.addAndGet(1).toString // generate tweetId.
        var tmp = new Tweets(tweetId, userId, msg, time)

        // extract mentions and store in tweet object
        var itr = regexMentions.findAllMatchIn(msg)
        while (itr.hasNext) {
          tmp.mentions += itr.next().toString.trim
        }

        // extract tags and store in tweet object
        itr = regexTags.findAllMatchIn(msg)
        while (itr.hasNext) {
          tmp.hashtags += itr.next().toString.trim
        }

        tweetStore.put(tweetId, tmp)

        var followers = users.get(userId).followers.iterator() // get all followers and add tweet to their timeline
        while (followers.hasNext()) {
          users.get(followers.next()).homeTimeline.add(tweetId)
        }
        users.get(userId).userTimeline.add(tweetId) // add to self timeline also
        sender ! tweetId

      case SendHomeTimeline(userId) =>
        var tweetIds = users.get(userId).homeTimeline
        var tmp: ArrayBuffer[Tweets] = ArrayBuffer.empty
        var itr = tweetIds.iterator()
        while (itr.hasNext()) {
          tmp += tweetStore.get(itr.next())
        }
        sender ! tmp.toList

      case SendUserTimeline(userId) =>
        var tweetIds = users.get(userId).userTimeline
        var tmp: ArrayBuffer[Tweets] = ArrayBuffer.empty
        var itr = tweetIds.iterator()
        while (itr.hasNext()) {
          tmp += tweetStore.get(itr.next())
        }
        sender ! tmp.toList

      case SendFollowers(userId) =>
        var idList = users.get(userId).followers
        var tmp: ArrayBuffer[sample] = ArrayBuffer.empty
        var itr = idList.iterator()
        while (itr.hasNext()) {
          var obj = users.get(itr.next())
          tmp += sample(obj.userId, obj.userName, obj.userTimeline.size(), obj.favorites.size(), obj.followers.size(), obj.following.size())
        }
        sender ! tmp.toList

      case SendFollowing(userId) =>
        var idList = users.get(userId).following
        var tmp: ArrayBuffer[sample] = ArrayBuffer.empty
        var itr = idList.iterator()
        while (itr.hasNext()) {
          var obj = users.get(itr.next())
          tmp += sample(obj.userId, obj.userName, obj.userTimeline.size(), obj.favorites.size(), obj.followers.size(), obj.following.size())
        }
        sender ! tmp.toList

      case SendUserProfile(userId) =>
        var obj = users.get(userId)
        sender ! sample(obj.userId, obj.userName, obj.userTimeline.size(), obj.favorites.size(), obj.followers.size(), obj.following.size())

      case SendMentions(userId) =>
        var mentionName = "@" + users.get(userId).userName
        var tmp: ArrayBuffer[Tweets] = ArrayBuffer.empty
        val itr = tweetStore.entrySet().iterator()
        while (itr.hasNext()) {
          val entry = itr.next();
          if (entry.getValue.mentions.contains(mentionName)) {
            tmp += entry.getValue()
          }
        }

        sender ! tmp.toList

      case _ => println("FAILED HERE 2")
    }
  }
}