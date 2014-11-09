package project4.src

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

trait Dist[A] {
  self =>
  def get: A

  def sample(n: Int): ArrayBuffer[A] = {
    ArrayBuffer.fill(n)(this.get)
  }

  def map[B](f: A => B): Dist[B] = new Dist[B] {
    override def get = f(self.get)
  }
}

class ClientNode(node: Int) {
  var id: Int = node
  var noOfFollowers = 0
  var followingUsers = 0
  var avgTweets = 0
  var name = "Client # " + id
  var arr = ArrayBuffer.empty[Int]
}

object sample {
  def main(args: Array[String]) {
    var avgTweetsPerSecond = args(0).toInt
    var noOfUsers = args(1).toInt

    // create a uniform random distribution object.
    val uniform = new Dist[Double] {
      // define the get method for Distribution object.
      private val rand = new java.util.Random()
      override def get = rand.nextDouble()
    }

    val gaussian = new Dist[Double] {
      // define the get method for Distribution object.
      private val rand = new java.util.Random()
      override def get = rand.nextGaussian()
    }

    def exponential(l: Double): Dist[Double] = {
      for {
        x <- uniform
      } yield math.log(x) * (-1 / l)
    }

    var clientList = ArrayBuffer.empty[ClientNode]
    for (i <- 1 to noOfUsers) {
      clientList += new ClientNode(i)
    }

    var ctr1 = 0
    // 307 => mean (avg tweets per user).	sample(size) => size is the no of Users.
    var TweetsPerUser = exponential(1.0 / 307.0).sample(noOfUsers).map(_.toInt)
    for (i <- 0 to noOfUsers - 1) {
      clientList(i).avgTweets = TweetsPerUser(i)
      ctr1 += TweetsPerUser(i)
    }
    clientList = clientList.sortBy(a => a.avgTweets)

    // we have data that people who tweet more have more followers. map the # of tweets to the followers.
    var FollowersPerUser = exponential(1.0 / 208.0).sample(noOfUsers).map(_.toInt)
    // since no of followers are roughly the same as no of following.
    var FollowingPerUser = exponential(1.0 / 208.0).sample(noOfUsers).map(_.toInt)

    FollowersPerUser = FollowersPerUser.sortBy(a => a)
    FollowingPerUser = FollowingPerUser.sortBy(a => a)
    for (i <- 1 to noOfUsers - 1) {
      clientList(i).noOfFollowers = FollowersPerUser(i)
      clientList(i).followingUsers = FollowingPerUser(i)
    }

    var duration = ctr1 / avgTweetsPerSecond

    var percent10 = (duration * 0.1).toInt
    var indexes = ArrayBuffer.empty[Int]
    val rnd = new Random

    for (i <- 1 to percent10) {
      var tmp = rnd.nextInt(duration)
      while (indexes.contains(tmp)) {
        tmp = rnd.nextInt(duration)
      }
      indexes += tmp
    }

    println(duration)

    var ctr4 = 0
    for (i <- 0 to noOfUsers - 1) {
      // Std. Deviation = Mean/4 (25%),		Mean = TweetsPerUser(i)
      var mean = clientList(i).avgTweets / duration.toDouble
      var tweetpersecondperuser = gaussian.map(_ * (mean / 4) + mean).sample(duration).map(a => Math.round(a).toInt)

      var skewedRate = tweetpersecondperuser.sortBy(a => a).takeRight(percent10).map(_ * 2) // double value of 10% of largest values to simulate peaks.
      for (j <- 0 to percent10 - 1) {
        tweetpersecondperuser(indexes(j)) = skewedRate(j)
      }
      clientList(i).arr = tweetpersecondperuser
      println("*********************** " + tweetpersecondperuser.sum + " ****")
      ctr4 += tweetpersecondperuser.sum
    }

    //duration will be the sample size for the twitter column distribution
    println("************* " + ctr1)
    println("************* " + ctr4)

  }
}